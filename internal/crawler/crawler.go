package crawler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
	// AssignDomain 为新域名建立副本分配，返回主副本节点 ID
	AssignDomain(domain string) string
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

	writer *batchWriter

	mu      sync.Mutex
	running bool
	stat    struct {
		fetched, indexed, failed, discovered int64
		skipped, duplicated                  int64
		lastErr                              string
		lastAt                               time.Time
	}

	// 站点权重缓存：原来每页查一次 seeds 表
	wMu  sync.RWMutex
	wMap map[string]float64
	wAt  time.Time

	// 精确内容去重：先查内存集合，未命中再落库核对
	dMu  sync.Mutex
	seen map[string]struct{}

	// 抓取并发上限，可在后台按节点单独调整并热生效
	concLimit atomic.Int64
	inflight  atomic.Int64
	maxPages  atomic.Int64
}

func New(cfg *config.Config, db *store.DB, cache *store.Cache, nodeID string, proxies []string, indexer Indexer) *Crawler {
	c := &Crawler{
		cfg:    cfg,
		db:     db,
		cache:  cache,
		nodeID: nodeID,
		fetcher: NewFetcher(cfg.Crawler.DefaultHeaders, proxies, cfg.Crawler.TimeoutSec,
			cfg.Crawler.RespectRobots, cfg.Crawler.PerDomainIntervalMS),
		frontier: NewFrontier(db, nodeID, cfg.Limits.CrawlQueueMax),
		indexer:  indexer,
		writer:   newBatchWriter(db),
	}
	c.frontier.setSink(c.writer)
	conc := cfg.Crawler.Concurrency
	if conc <= 0 {
		conc = 8
	}
	c.concLimit.Store(int64(conc))
	c.maxPages.Store(int64(cfg.Crawler.MaxPagesPerRun))
	return c
}

// SetMaxPages 运行时调整单次运行抓取上限（0 表示不限）。
func (c *Crawler) SetMaxPages(n int) {
	if n < 0 {
		n = 0
	}
	c.maxPages.Store(int64(n))
}

// SetConcurrency 运行时调整本节点抓取并发（线程数），下一次派发立即生效。
func (c *Crawler) SetConcurrency(n int) {
	if n <= 0 {
		return
	}
	if n > 512 {
		n = 512
	}
	c.concLimit.Store(int64(n))
}

// Concurrency 当前生效的并发数。
func (c *Crawler) Concurrency() int { return int(c.concLimit.Load()) }

// SetPerDomainIntervalMS 运行时调整同域请求间隔。
func (c *Crawler) SetPerDomainIntervalMS(ms int) { c.fetcher.SetPerDomainIntervalMS(ms) }

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

type indexJob struct {
	doc    *store.Document
	tokens []string
}

// Run 以流水线方式抓取：调度器按域名时隙派发，抓取 worker 只管取页面，
// 索引写入交给独立 worker，互不阻塞。
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

	// 固定 worker 池，实际并发由 concLimit 控制，后台改配置无需重启
	const maxWorkers = 256
	work := make(chan Task, maxWorkers)
	idxCh := make(chan indexJob, 2048)

	var fetchWG sync.WaitGroup
	for i := 0; i < maxWorkers; i++ {
		fetchWG.Add(1)
		go func() {
			defer fetchWG.Done()
			for t := range work {
				c.process(ctx, t, idxCh)
				c.inflight.Add(-1)
			}
		}()
	}

	// 索引写库与抓取解耦：抓取不再等事务提交，索引也可批量攒批
	var idxWG sync.WaitGroup
	for i := 0; i < 4; i++ {
		idxWG.Add(1)
		go func() {
			defer idxWG.Done()
			for j := range idxCh {
				c.writeIndex(j)
			}
		}()
	}

	c.dispatch(ctx, work)

	close(work)
	fetchWG.Wait()
	close(idxCh)
	idxWG.Wait()
}

// dispatch 只把「该域名此刻允许发起请求」的任务放进 work，
// 否则留在内存里等下一轮，worker 不会卡在 sleep 上白占并发。
func (c *Crawler) dispatch(ctx context.Context, out chan<- Task) {
	// 按域名分桶：队列里同一站点的链接往往成百上千连在一起，
	// 若按顺序派发会被单个域名的间隔限制卡死（曾经只有 5 页/秒）。
	const poolMax = 512
	const lowWater = 256

	buckets := map[string][]Task{}
	lastDomains := time.Time{}
	lastLog := time.Now()
	for {
		if ctx.Err() != nil {
			return
		}
		// 新入队的域名先落副本分配，否则不会出现在本节点的可领取白名单里
		if c.owner != nil {
			for _, d := range c.frontier.DrainNewDomains() {
				c.owner.AssignDomain(d)
			}
			if time.Since(lastDomains) > 5*time.Second {
				c.frontier.SetDomains(c.owner.OwnedDomains())
				lastDomains = time.Now()
			}
		}

		limit := c.concLimit.Load()
		sent := 0
		for dom, list := range buckets {
			if c.inflight.Load() >= limit {
				break
			}
			if len(list) == 0 {
				delete(buckets, dom)
				continue
			}
			if !c.fetcher.TakeSlot(dom) {
				continue
			}
			select {
			case out <- list[0]:
				c.inflight.Add(1)
				sent++
				buckets[dom] = list[1:]
			case <-ctx.Done():
				return
			}
		}

		total := 0
		for _, l := range buckets {
			total += len(l)
		}
		if total < lowWater && !c.overLimit() {
			n := poolMax - total
			if n > 64 {
				n = 64
			}
			if tasks, err := c.frontier.Lease(ctx, n); err == nil && len(tasks) > 0 {
				for _, t := range tasks {
					buckets[t.Domain] = append(buckets[t.Domain], t)
				}
			}
		}

		if debugDispatch && time.Since(lastLog) > time.Second {
			lastLog = time.Now()
			log.Printf("[dispatch] sent=%d buckets=%d total=%d inflight=%d limit=%d", sent, len(buckets), total, c.inflight.Load(), limit)
		}

		switch {
		case sent > 0:
			// 有进展，立即继续
		case total == 0:
			sleepCtx(ctx, 250*time.Millisecond)
		default:
			// 全在等各域名时隙，短暂等待后重试
			sleepCtx(ctx, 15*time.Millisecond)
		}
	}
}

var debugDispatch = os.Getenv("QZ_DEBUG_DISPATCH") != ""

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (c *Crawler) overLimit() bool {
	max := c.maxPages.Load()
	if max <= 0 {
		return false
	}
	c.mu.Lock()
	n := c.stat.fetched
	c.mu.Unlock()
	return n >= max
}

func (c *Crawler) process(ctx context.Context, t Task, idxCh chan<- indexJob) {
	if !c.domainAllowed(t.Domain) {
		c.frontier.Done(ctx, t.ID)
		return
	}
	// 多节点下域名按主副本归属：非主副本不重复抓取同一份数据
	if !c.owns(t.Domain) {
		c.frontier.Release(ctx, t.ID)
		return
	}
	res := c.fetcher.FetchScheduled(ctx, t.URL)

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
		c.bumpSkip()
		c.frontier.Done(ctx, t.ID)
		return
	}
	// 导航页 / 列表页：没定位到正文容器且链接文字占比高，不入索引（链接仍参与扩散）
	if !page.Confident && page.LinkDensity > 0.45 {
		c.bumpSkip()
		c.frontier.Done(ctx, t.ID)
		return
	}

	ch := sha256.Sum256([]byte(page.Text))
	contentHash := hex.EncodeToString(ch[:])

	tokens := algo.Tokenize(page.Title + " " + page.Text)

	// 链接扩散先做，不等索引
	if c.cfg.Crawler.Mode == "full" && t.Depth < c.cfg.Crawler.MaxDepth {
		tasks := make([]Task, 0, len(page.Links))
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

	// 精确内容去重：内存集合挡住绝大多数重复页，未命中再落库核对
	if c.alreadySeen(ctx, contentHash) {
		c.mu.Lock()
		c.stat.duplicated++
		c.mu.Unlock()
		return
	}

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

	select {
	case idxCh <- indexJob{doc: doc, tokens: tokens}:
	case <-ctx.Done():
	}
}

func (c *Crawler) bumpSkip() {
	c.mu.Lock()
	c.stat.skipped++
	c.mu.Unlock()
}

func (c *Crawler) writeIndex(j indexJob) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := c.indexer.IndexDocument(ctx, j.doc, j.tokens); err != nil {
		c.mu.Lock()
		c.stat.failed++
		c.stat.lastErr = err.Error()
		c.mu.Unlock()
		return
	}
	c.mu.Lock()
	c.stat.indexed++
	c.mu.Unlock()
}

// alreadySeen 判断内容是否已入库；内存命中直接返回，未命中查库并回填。
func (c *Crawler) alreadySeen(ctx context.Context, hash string) bool {
	c.dMu.Lock()
	if _, ok := c.seen[hash]; ok {
		c.dMu.Unlock()
		return true
	}
	c.dMu.Unlock()

	var id int64
	err := c.db.QueryRowContext(ctx, `SELECT id FROM documents WHERE content_hash=? LIMIT 1`, hash).Scan(&id)
	if err == nil && id > 0 {
		c.rememberHash(hash)
		return true
	}
	return false
}

func (c *Crawler) rememberHash(hash string) {
	c.dMu.Lock()
	if c.seen == nil {
		c.seen = make(map[string]struct{}, 1024)
	}
	if len(c.seen) > 500000 {
		// 简单裁剪：超限后清空，重复内容由数据库唯一键兜底
		c.seen = make(map[string]struct{}, 1024)
	}
	c.seen[hash] = struct{}{}
	c.dMu.Unlock()
}

func (c *Crawler) domainWeight(ctx context.Context, domain string) float64 {
	c.wMu.RLock()
	if c.wMap != nil && time.Since(c.wAt) < 60*time.Second {
		w := c.wMap[domain]
		c.wMu.RUnlock()
		if w <= 0 {
			w = 1
		}
		return w
	}
	c.wMu.RUnlock()

	c.wMu.Lock()
	defer c.wMu.Unlock()
	if c.wMap != nil && time.Since(c.wAt) < 60*time.Second {
		if w := c.wMap[domain]; w > 0 {
			return w
		}
		return 1
	}
	next := map[string]float64{}
	rows, err := c.db.QueryContext(ctx, `SELECT domain, MAX(weight) FROM seeds GROUP BY domain`)
	if err == nil {
		for rows.Next() {
			var d string
			var w float64
			if rows.Scan(&d, &w) == nil && w > 0 {
				next[d] = w
			}
		}
		rows.Close()
	}
	c.wMap, c.wAt = next, time.Now()
	if w := next[domain]; w > 0 {
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
	c.writer.log(logRec{
		url: t.URL, domain: t.Domain, status: statusOf(res), httpCode: res.Status,
		bytes: len(res.Body), errStr: errStr, durMS: res.DurationMS, nodeID: c.nodeID, ruleID: ruleID,
	})
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
		"skipped":    c.stat.skipped,
		"duplicated": c.stat.duplicated,
		"inflight":   c.inflight.Load(),
		"concurrency": c.concLimit.Load(),
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
		x := fnv1a64(t)
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

// fnv1a64 内联 FNV-1a，避免每个词项都分配一个 hash.Hash。
func fnv1a64(s string) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}
