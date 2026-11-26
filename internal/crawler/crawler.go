package crawler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"strings"
	"sync"
	"time"

	"queryza/internal/algo"
	"queryza/internal/config"
	"queryza/internal/store"
)

// Indexer 由索引层实现，避免包循环依赖。
type Indexer interface {
	IndexDocument(ctx context.Context, doc *store.Document, tokens []string) error
}

// Ownership 判断域名是否由本节点负责（主副本），用于多节点分片。
type Ownership interface {
	OwnDomain(domain string) bool
	OwnedDomains() []string
}

type Crawler struct {
	cfg      *config.Config
	db       *store.DB
	cache    *store.Cache
	nodeID   string
	fetcher  *Fetcher
	frontier *Frontier
	indexer  Indexer
	owner    Ownership

	mu      sync.Mutex
	running bool
	stat    struct {
		fetched, indexed, failed, discovered int64
		lastErr                              string
		lastAt                               time.Time
	}
}

func New(cfg *config.Config, db *store.DB, cache *store.Cache, nodeID string, proxies []string, indexer Indexer) *Crawler {
	return &Crawler{
		cfg:    cfg,
		db:     db,
		cache:  cache,
		nodeID: nodeID,
		fetcher: NewFetcher(cfg.Crawler.DefaultHeaders, proxies, cfg.Crawler.TimeoutSec,
			cfg.Crawler.RespectRobots, cfg.Crawler.PerDomainIntervalMS),
		frontier: NewFrontier(db, nodeID, cfg.Limits.CrawlQueueMax),
		indexer:  indexer,
	}
}

type seedDef struct {
	URL      string  `json:"url"`
	Name     string  `json:"name"`
	Category string  `json:"category"`
	Weight   float64 `json:"weight"`
	Depth    int     `json:"depth"`
	Enabled  *bool   `json:"enabled"`
}

// LoadSeeds 导入基本索引表（种子站点），并把启用的种子入队。
func (c *Crawler) LoadSeeds(ctx context.Context) (int, error) {
	path := c.cfg.Crawler.SeedFile
	if path == "" {
		path = "seeds/seeds.json"
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("读取种子文件失败: %w", err)
	}
	var seeds []seedDef
	if err := json.Unmarshal(b, &seeds); err != nil {
		return 0, fmt.Errorf("解析种子文件失败: %w", err)
	}
	added := 0
	var tasks []Task
	for _, s := range seeds {
		host := hostOf(s.URL)
		if host == "" {
			continue
		}
		enabled := true
		if s.Enabled != nil {
			enabled = *s.Enabled
		}
		if s.Weight <= 0 {
			s.Weight = 1
		}
		if s.Depth <= 0 {
			s.Depth = c.cfg.Crawler.MaxDepth
		}
		_, err := c.db.ExecContext(ctx, `INSERT IGNORE INTO seeds(url,url_hash,domain,name,category,weight,enabled,depth)
			VALUES(?,?,?,?,?,?,?,?)`, s.URL, URLHash(s.URL), host, s.Name, s.Category, s.Weight, enabled, s.Depth)
		if err != nil {
			continue
		}
		added++
		if enabled && c.domainAllowed(host) {
			tasks = append(tasks, Task{
				URL: s.URL, URLHash: URLHash(s.URL), Domain: host,
				Depth: 0, Priority: int(s.Weight * 100),
			})
		}
	}
	_, _ = c.frontier.Enqueue(ctx, tasks)
	return added, nil
}

func (c *Crawler) domainAllowed(host string) bool {
	if host == "" {
		return false
	}
	for _, b := range c.cfg.Crawler.BlockDomains {
		if host == b || strings.HasSuffix(host, "."+b) {
			return false
		}
	}
	// allow_domains 非空时作为白名单（两种模式均生效）；为空则不限制
	if len(c.cfg.Crawler.AllowDomains) > 0 {
		for _, a := range c.cfg.Crawler.AllowDomains {
			if host == a || strings.HasSuffix(host, "."+a) {
				return true
			}
		}
		return false
	}
	return true
}

func (c *Crawler) Run(ctx context.Context) {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return
	}
	c.running = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.running = false
		c.mu.Unlock()
	}()

	c.frontier.ResetStuck(ctx, 5*time.Minute)
	conc := c.cfg.Crawler.Concurrency
	if conc <= 0 {
		conc = 4
	}
	sem := make(chan struct{}, conc)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// 每轮检查是否超过单次运行上限
		c.mu.Lock()
		fetched := c.stat.fetched
		c.mu.Unlock()
		if c.cfg.Crawler.MaxPagesPerRun > 0 && fetched >= int64(c.cfg.Crawler.MaxPagesPerRun) {
			time.Sleep(2 * time.Second)
			continue
		}

		if c.owner != nil {
			c.frontier.SetDomains(c.owner.OwnedDomains())
		}
		tasks, err := c.frontier.Lease(ctx, conc)
		if err != nil || len(tasks) == 0 {
			time.Sleep(800 * time.Millisecond)
			continue
		}
		var wg sync.WaitGroup
		for _, t := range tasks {
			wg.Add(1)
			sem <- struct{}{}
			go func(t Task) {
				defer wg.Done()
				defer func() { <-sem }()
				c.process(ctx, t)
			}(t)
		}
		wg.Wait()
	}
}

func (c *Crawler) process(ctx context.Context, t Task) {
	if !c.domainAllowed(t.Domain) {
		c.frontier.Done(ctx, t.ID)
		return
	}
	// 多节点下域名按主副本归属：非主副本不重复抓取同一份数据
	if !c.owns(t.Domain) {
		c.frontier.Release(ctx, t.ID)
		return
	}
	res := c.fetcher.Fetch(ctx, t.URL)

	// 先解析，日志里带上命中的清洗规则，便于观察规则库效果
	var page *Page
	ruleID := ""
	okBody := res.Err == nil && res.Status >= 200 && res.Status < 400 &&
		(res.ContentType == "" || strings.Contains(strings.ToLower(res.ContentType), "html"))
	if okBody {
		page = Parse(t.URL, res.Body)
		ruleID = page.Rule
	}
	c.logFetch(ctx, t, res, ruleID)

	c.mu.Lock()
	c.stat.fetched++
	c.stat.lastAt = time.Now()
	c.mu.Unlock()

	if res.Err != nil || res.Status < 200 || res.Status >= 400 {
		c.mu.Lock()
		c.stat.failed++
		if res.Err != nil {
			c.stat.lastErr = res.Err.Error()
		} else {
			c.stat.lastErr = fmt.Sprintf("HTTP %d", res.Status)
		}
		c.mu.Unlock()
		c.frontier.Fail(ctx, t.ID, 0)
		return
	}
	if page == nil {
		c.frontier.Done(ctx, t.ID)
		return
	}
	if len(page.Text) < 30 {
		c.frontier.Done(ctx, t.ID)
		return
	}
	// 导航页 / 列表页：没定位到正文容器且链接文字占比高，不入索引（链接仍参与扩散）
	if !page.Confident && page.LinkDensity > 0.45 {
		c.frontier.Done(ctx, t.ID)
		return
	}

	ch := sha256.Sum256([]byte(page.Text))
	contentHash := hex.EncodeToString(ch[:])

	// 精确内容去重：已存在同内容页面则跳过索引
	var exists int64
	c.db.QueryRowContext(ctx, `SELECT id FROM documents WHERE content_hash=? LIMIT 1`, contentHash).Scan(&exists)

	tokens := algo.Tokenize(page.Title + " " + page.Text)
	doc := &store.Document{
		URL:           t.URL,
		URLHash:       t.URLHash,
		Domain:        t.Domain,
		Title:         page.Title,
		Content:       page.Text,
		ContentLen:    len(tokens),
		HTTPStatus:    res.Status,
		FetchedAt:     time.Now(),
		ContentHash:   contentHash,
		SimHash:       simhash64(tokens),
		SiteWeight:    c.domainWeight(ctx, t.Domain),
		DynamicWeight: 1,
		IsHot:         false,
		IndexStatus:   0,
		CreatedAt:     time.Now(),
	}

	if exists == 0 {
		if err := c.indexer.IndexDocument(ctx, doc, tokens); err != nil {
			c.mu.Lock()
			c.stat.failed++
			c.stat.lastErr = err.Error()
			c.mu.Unlock()
			c.frontier.Fail(ctx, t.ID, 1)
			return
		}
		c.mu.Lock()
		c.stat.indexed++
		c.mu.Unlock()
	}

	// 仅全网模式启用域名自发现
	if c.cfg.Crawler.Mode == "full" && t.Depth < c.cfg.Crawler.MaxDepth {
		var tasks []Task
		seedsDepth := t.Depth + 1
		for _, link := range page.Links {
			h := hostOf(link)
			if !c.domainAllowed(h) {
				continue
			}
			tasks = append(tasks, Task{URL: link, URLHash: URLHash(link), Domain: h, Depth: seedsDepth, Priority: 10})
		}
		if n, _ := c.frontier.Enqueue(ctx, tasks); n > 0 {
			c.mu.Lock()
			c.stat.discovered += int64(n)
			c.mu.Unlock()
		}
	}

	c.frontier.Done(ctx, t.ID)
}

func (c *Crawler) domainWeight(ctx context.Context, domain string) float64 {
	var w float64
	if err := c.db.QueryRowContext(ctx, `SELECT weight FROM seeds WHERE domain=? LIMIT 1`, domain).Scan(&w); err == nil && w > 0 {
		return w
	}
	return 1.0
}

func (c *Crawler) logFetch(ctx context.Context, t Task, res *FetchResult, ruleID string) {
	errStr := ""
	if res.Err != nil {
		errStr = res.Err.Error()
	}
	if len(errStr) > 500 {
		errStr = errStr[:500]
	}
	c.db.ExecContext(ctx, `INSERT INTO crawl_logs(url,domain,status,http_code,bytes,error,duration_ms,node_id,rule_id)
		VALUES(?,?,?,?,?,?,?,?,?)`, t.URL, t.Domain, statusOf(res), res.Status, len(res.Body), errStr, res.DurationMS, c.nodeID, ruleID)
}

func statusOf(res *FetchResult) string {
	if res.Err != nil {
		return "error"
	}
	if res.Status >= 200 && res.Status < 400 {
		return "ok"
	}
	return "http_error"
}

func (c *Crawler) Stats() map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return map[string]interface{}{
		"running":    c.running,
		"fetched":    c.stat.fetched,
		"indexed":    c.stat.indexed,
		"failed":     c.stat.failed,
		"discovered": c.stat.discovered,
		"last_error": c.stat.lastErr,
		"last_at":    c.stat.lastAt,
		"mode":       c.cfg.Crawler.Mode,
	}
}

func (c *Crawler) EnqueueURLs(ctx context.Context, urls []string, depth, priority int) (int, error) {
	var tasks []Task
	for _, u := range urls {
		h := hostOf(u)
		if h == "" || !c.domainAllowed(h) {
			continue
		}
		tasks = append(tasks, Task{URL: u, URLHash: URLHash(u), Domain: h, Depth: depth, Priority: priority})
	}
	return c.frontier.Enqueue(ctx, tasks)
}

func (c *Crawler) Frontier() *Frontier { return c.frontier }

// SetOwnership 设置域名归属判断；为空表示单机模式，全部由本节点处理。
func (c *Crawler) SetOwnership(o Ownership) { c.owner = o }

func (c *Crawler) owns(domain string) bool {
	if c.owner == nil {
		return true
	}
	return c.owner.OwnDomain(domain)
}

func simhash64(tokens []string) int64 {
	var v [64]int
	for _, t := range tokens {
		h := fnv.New64a()
		h.Write([]byte(t))
		x := h.Sum64()
		for i := 0; i < 64; i++ {
			if x&(1<<uint(i)) != 0 {
				v[i]++
			} else {
				v[i]--
			}
		}
	}
	var f uint64
	for i := 0; i < 64; i++ {
		if v[i] > 0 {
			f |= 1 << uint(i)
		}
	}
	return int64(f)
}
