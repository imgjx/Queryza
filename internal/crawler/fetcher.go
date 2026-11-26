package crawler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type FetchResult struct {
	URL         string
	FinalURL    string
	Status      int
	Body        []byte
	ContentType string
	Err         error
	DurationMS  int
	ViaProxy    string
}

// Fetcher 负责单页抓取：可配请求头、代理组（轮询）、robots 遵循。
type Fetcher struct {
	headers       map[string]string
	proxies       []string
	rr            uint64
	timeout       time.Duration
	respectRobots bool
	clients       sync.Map // proxy -> *http.Client
	robots        sync.Map // host -> *robotsEntry
	noProxyClient *http.Client

	// 同域最小请求间隔，避免把站点打出 429
	gateMu   sync.Mutex
	gateNext map[string]time.Time
	gateGapN atomic.Int64 // 同域最小间隔（纳秒），支持运行时热更
}

type robotsEntry struct {
	rules   []robotsRule
	expires time.Time
}

type robotsRule struct {
	allow bool
	path  string
	delay time.Duration
}

func NewFetcher(headers map[string]string, proxies []string, timeoutSec int, respectRobots bool, perDomainMS int) *Fetcher {
	if timeoutSec <= 0 {
		timeoutSec = 15
	}
	f := &Fetcher{
		headers:       headers,
		proxies:       proxies,
		timeout:       time.Duration(timeoutSec) * time.Second,
		respectRobots: respectRobots,
		gateNext:      map[string]time.Time{},
	}
	f.gateGapN.Store(int64(perDomainMS) * int64(time.Millisecond))
	f.noProxyClient = &http.Client{Timeout: f.timeout, Transport: f.transport("")}
	return f
}

func (f *Fetcher) transport(proxy string) *http.Transport {
	t := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     60 * time.Second,
		DisableCompression:  false,
	}
	if proxy != "" {
		if pu, err := url.Parse(proxy); err == nil {
			t.Proxy = http.ProxyURL(pu)
		}
	}
	return t
}

func (f *Fetcher) client(proxy string) *http.Client {
	if proxy == "" {
		return f.noProxyClient
	}
	if v, ok := f.clients.Load(proxy); ok {
		return v.(*http.Client)
	}
	c := &http.Client{Timeout: f.timeout, Transport: f.transport(proxy)}
	f.clients.Store(proxy, c)
	return c
}

func (f *Fetcher) pickProxy() string {
	if len(f.proxies) == 0 {
		return ""
	}
	n := atomic.AddUint64(&f.rr, 1)
	return f.proxies[int(n)%len(f.proxies)]
}

func (f *Fetcher) Fetch(ctx context.Context, rawURL string) *FetchResult {
	return f.fetch(ctx, rawURL, true)
}

// FetchScheduled 用于调度器已经通过 TakeSlot 预约过时隙的任务，不再二次等待。
func (f *Fetcher) FetchScheduled(ctx context.Context, rawURL string) *FetchResult {
	return f.fetch(ctx, rawURL, false)
}

// TakeSlot 若该域名当前可发起请求，则预约下一个时隙并返回 true。
// 由调度器在派发前调用，把「等间隔」放到派发侧，避免 worker 空转。
func (f *Fetcher) TakeSlot(host string) bool {
	gap := f.gap()
	if gap <= 0 || host == "" {
		return true
	}
	f.gateMu.Lock()
	defer f.gateMu.Unlock()
	now := time.Now()
	if next := f.gateNext[host]; next.After(now) {
		return false
	}
	f.gateNext[host] = now.Add(gap)
	return true
}

func (f *Fetcher) fetch(ctx context.Context, rawURL string, throttle bool) *FetchResult {
	start := time.Now()
	res := &FetchResult{URL: rawURL}

	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		res.Err = fmt.Errorf("非法 URL")
		res.DurationMS = int(time.Since(start).Milliseconds())
		return res
	}

	if throttle && !f.waitDomain(ctx, u.Host) {
		res.Err = ctx.Err()
		res.DurationMS = int(time.Since(start).Milliseconds())
		return res
	}

	if f.respectRobots && !f.allowed(ctx, u) {
		res.Err = fmt.Errorf("robots.txt 禁止抓取")
		res.Status = -1
		res.DurationMS = int(time.Since(start).Milliseconds())
		return res
	}

	proxy := f.pickProxy()
	res.ViaProxy = proxy
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		res.Err = err
		return res
	}
	for k, v := range f.headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "QueryzaBot/1.0 (+https://example.local/bot)")
	}

	resp, err := f.client(proxy).Do(req)
	if err != nil {
		res.Err = err
		res.DurationMS = int(time.Since(start).Milliseconds())
		return res
	}
	defer resp.Body.Close()

	res.Status = resp.StatusCode
	res.FinalURL = resp.Request.URL.String()
	res.ContentType = resp.Header.Get("Content-Type")
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20)) // 单页上限 8MB
	if err != nil {
		res.Err = err
	}
	res.Body = body
	res.DurationMS = int(time.Since(start).Milliseconds())
	return res
}

// allowed 基于 robots.txt 判断；抓取失败/不存在时默认放行。
func (f *Fetcher) allowed(ctx context.Context, u *url.URL) bool {
	host := u.Host
	if v, ok := f.robots.Load(host); ok {
		e := v.(*robotsEntry)
		if time.Now().Before(e.expires) {
			return matchRobots(e.rules, u.Path)
		}
	}
	rules := f.loadRobots(ctx, u)
	f.robots.Store(host, &robotsEntry{rules: rules, expires: time.Now().Add(time.Hour)})
	return matchRobots(rules, u.Path)
}

func (f *Fetcher) loadRobots(ctx context.Context, u *url.URL) []robotsRule {
	robotsURL := u.Scheme + "://" + u.Host + "/robots.txt"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, robotsURL, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("User-Agent", "QueryzaBot/1.0")
	resp, err := f.client("").Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512<<10))
	return parseRobots(string(b))
}

func parseRobots(body string) []robotsRule {
	var rules []robotsRule
	inStar := false
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, ":")
		if idx < 0 {
			continue
		}
		k := strings.ToLower(strings.TrimSpace(line[:idx]))
		v := strings.TrimSpace(line[idx+1:])
		switch k {
		case "user-agent":
			inStar = (v == "*")
		case "disallow":
			if inStar && v != "" {
				rules = append(rules, robotsRule{allow: false, path: v})
			}
		case "allow":
			if inStar && v != "" {
				rules = append(rules, robotsRule{allow: true, path: v})
			}
		case "crawl-delay":
			if inStar {
				if d, err := time.ParseDuration(v + "s"); err == nil {
					rules = append(rules, robotsRule{allow: true, path: "", delay: d})
				}
			}
		}
	}
	return rules
}

func matchRobots(rules []robotsRule, path string) bool {
	bestLen := -1
	allow := true
	for _, r := range rules {
		if r.path == "" {
			continue
		}
		if strings.HasPrefix(path, r.path) {
			if len(r.path) > bestLen {
				bestLen = len(r.path)
				allow = r.allow
			}
		}
	}
	return allow
}

// waitDomain 保证同一域名两次请求之间至少间隔 gateGap。
func (f *Fetcher) waitDomain(ctx context.Context, host string) bool {
	gap := f.gap()
	if gap <= 0 || host == "" {
		return true
	}
	f.gateMu.Lock()
	next := f.gateNext[host]
	now := time.Now()
	if next.Before(now) {
		next = now
	}
	f.gateNext[host] = next.Add(gap)
	f.gateMu.Unlock()

	if d := time.Until(next); d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
		}
	}
	return true
}

// SetPerDomainIntervalMS 运行时调整同域请求间隔（0 表示不限制）。
func (f *Fetcher) SetPerDomainIntervalMS(ms int) {
	if ms < 0 {
		ms = 0
	}
	f.gateGapN.Store(int64(ms) * int64(time.Millisecond))
}

func (f *Fetcher) gap() time.Duration { return time.Duration(f.gateGapN.Load()) }
