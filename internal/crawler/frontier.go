package crawler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strings"
	"sync"
	"time"

	"queryza/internal/store"
)

type Task struct {
	ID       int64
	URL      string
	URLHash  string
	Domain   string
	Shard    int
	Depth    int
	Priority int
}

type Frontier struct {
	db      *store.DB
	nodeID  string
	max     int
	domains []string // 本节点负责的域名，非空时只领取这些域名的任务
	sink    *batchWriter

	// 队列长度缓存：原来每次入队都 COUNT(*) 全表
	cMu     sync.Mutex
	cCount  int64
	cAt     time.Time

	// 新出现的域名：派发前统一做副本分配，避免「未分配 → 不在白名单 → 永不抓取」
	dMu     sync.Mutex
	newDoms map[string]struct{}

	// 环形游标：按域名轮转领取，避免同一站点占满批次
	cursor string
}

// DrainNewDomains 取出自上次调用以来新入队的域名。
func (f *Frontier) DrainNewDomains() []string {
	f.dMu.Lock()
	defer f.dMu.Unlock()
	if len(f.newDoms) == 0 {
		return nil
	}
	out := make([]string, 0, len(f.newDoms))
	for d := range f.newDoms {
		out = append(out, d)
	}
	f.newDoms = map[string]struct{}{}
	return out
}

func (f *Frontier) noteDomains(tasks []Task) {
	if len(tasks) == 0 {
		return
	}
	f.dMu.Lock()
	if f.newDoms == nil {
		f.newDoms = make(map[string]struct{}, 64)
	}
	for _, t := range tasks {
		if t.Domain != "" {
			f.newDoms[t.Domain] = struct{}{}
		}
	}
	f.dMu.Unlock()
}

func NewFrontier(db *store.DB, nodeID string, max int) *Frontier {
	if max <= 0 {
		max = 500000
	}
	return &Frontier{db: db, nodeID: nodeID, max: max}
}

func (f *Frontier) setSink(w *batchWriter) { f.sink = w }

func URLHash(u string) string {
	h := sha256.Sum256([]byte(u))
	return hex.EncodeToString(h[:])
}

func hostOf(u string) string {
	pu, err := url.Parse(u)
	if err != nil {
		return ""
	}
	return pu.Host
}

// Enqueue 批量入队，已存在则忽略。返回新增数量。
func (f *Frontier) Enqueue(ctx context.Context, tasks []Task) (int, error) {
	if len(tasks) == 0 {
		return 0, nil
	}
	room := f.max - int(f.queueLen(ctx))
	if room <= 0 {
		return 0, nil
	}
	if room < len(tasks) {
		tasks = tasks[:room]
	}
	added := 0
	// 多值 INSERT，几百个链接一次往返；分批避免 SQL 过长
	const chunk = 200
	for start := 0; start < len(tasks); start += chunk {
		end := start + chunk
		if end > len(tasks) {
			end = len(tasks)
		}
		batch := tasks[start:end]
		var sb strings.Builder
		sb.Grow(len(batch) * 24)
		sb.WriteString(`INSERT IGNORE INTO crawl_queue(url,url_hash,domain,shard,depth,priority,status,node_id) VALUES `)
		args := make([]interface{}, 0, len(batch)*6)
		for i, t := range batch {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(`(?,?,?,?,?,?,'pending','')`)
			if t.URLHash == "" {
				t.URLHash = URLHash(t.URL)
			}
			args = append(args, t.URL, t.URLHash, t.Domain, t.Shard, t.Depth, t.Priority)
		}
		r, err := f.db.ExecContext(ctx, sb.String(), args...)
		if err != nil {
			continue
		}
		if n, _ := r.RowsAffected(); n > 0 {
			added += int(n)
		}
	}
	if added > 0 {
		f.noteDomains(tasks)
		f.cMu.Lock()
		f.cCount += int64(added)
		f.cAt = time.Now()
		f.cMu.Unlock()
	}
	return added, nil
}

// queueLen 队列长度，30 秒缓存并随入队增量更新。
func (f *Frontier) queueLen(ctx context.Context) int64 {
	f.cMu.Lock()
	if time.Since(f.cAt) < 30*time.Second {
		n := f.cCount
		f.cMu.Unlock()
		return n
	}
	f.cMu.Unlock()

	var cnt int64
	f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM crawl_queue`).Scan(&cnt)
	f.cMu.Lock()
	f.cCount, f.cAt = cnt, time.Now()
	f.cMu.Unlock()
	return cnt
}

// SetDomains 限定本节点可领取的域名；传 nil 表示不限制。
func (f *Frontier) SetDomains(ds []string) { f.domains = ds }

// Lease 原子领取任务。
// 按域名排序 + 环形游标领取：同一站点的链接在队列里往往成百上千连在一起，
// 若按优先级顺序领，会被单个站点占满批次，抓取并发退化成 1-2 个域名。
func (f *Frontier) Lease(ctx context.Context, n int) ([]Task, error) {
	if n <= 0 {
		n = 1
	}
	tasks, err := f.leaseFrom(ctx, f.cursor, n)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 && f.cursor != "" {
		// 游标已到队尾，回绕到队首
		f.cursor = ""
		return f.leaseFrom(ctx, "", n)
	}
	if len(tasks) > 0 {
		f.cursor = tasks[len(tasks)-1].Domain
	}
	return tasks, nil
}

func (f *Frontier) leaseFrom(ctx context.Context, after string, n int) ([]Task, error) {
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	q := `SELECT id,url,url_hash,domain,shard,depth,priority FROM crawl_queue WHERE status='pending'`
	var args []interface{}
	if len(f.domains) > 0 && len(f.domains) <= 800 {
		q += ` AND domain IN (` + placeholders(len(f.domains)) + `)`
		for _, d := range f.domains {
			args = append(args, d)
		}
	}
	if after != "" {
		q += ` AND domain > ?`
		args = append(args, after)
	}
	q += ` ORDER BY domain ASC, priority DESC, id ASC LIMIT ? FOR UPDATE SKIP LOCKED`
	args = append(args, n)

	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	var tasks []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.URL, &t.URLHash, &t.Domain, &t.Shard, &t.Depth, &t.Priority); err == nil {
			tasks = append(tasks, t)
		}
	}
	rows.Close()
	if len(tasks) == 0 {
		return nil, nil
	}
	// 批量改状态，一条 SQL 代替逐行 UPDATE
	var sb strings.Builder
	sb.WriteString(`UPDATE crawl_queue SET status='fetching', node_id=?, updated_at=NOW() WHERE id IN (`)
	uargs := make([]interface{}, 0, len(tasks)+1)
	uargs = append(uargs, f.nodeID)
	for i, t := range tasks {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('?')
		uargs = append(uargs, t.ID)
	}
	sb.WriteByte(')')
	if _, err := tx.ExecContext(ctx, sb.String(), uargs...); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return tasks, nil
}

// Release 把任务放回队列（非本节点负责的域名用）
func (f *Frontier) Release(ctx context.Context, id int64) {
	if f.sink != nil {
		f.sink.queue(id, "pending", -1)
		return
	}
	f.db.ExecContext(ctx, `UPDATE crawl_queue SET status='pending', updated_at=NOW() WHERE id=?`, id)
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, 0, n*2)
	for i := 0; i < n; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '?')
	}
	return string(b)
}

func (f *Frontier) Done(ctx context.Context, id int64) {
	if f.sink != nil {
		f.sink.queue(id, "done", -1)
		return
	}
	f.db.ExecContext(ctx, `UPDATE crawl_queue SET status='done', updated_at=NOW() WHERE id=?`, id)
}

func (f *Frontier) Fail(ctx context.Context, id int64, retries int) {
	status := "pending"
	if retries >= 3 {
		status = "error"
	}
	if f.sink != nil {
		f.sink.queue(id, status, retries)
		return
	}
	f.db.ExecContext(ctx, `UPDATE crawl_queue SET status=?, retries=?, updated_at=NOW() WHERE id=?`, status, retries, id)
}

func (f *Frontier) ResetStuck(ctx context.Context, olderThan time.Duration) {
	f.db.ExecContext(ctx, `UPDATE crawl_queue SET status='pending', updated_at=NOW()
		WHERE status='fetching' AND updated_at < DATE_SUB(NOW(), INTERVAL ? SECOND)`, int(olderThan.Seconds()))
}

func (f *Frontier) Stats(ctx context.Context) map[string]int64 {
	out := map[string]int64{"pending": 0, "fetching": 0, "done": 0, "error": 0}
	rows, err := f.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM crawl_queue GROUP BY status`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var s string
		var c int64
		if rows.Scan(&s, &c) == nil {
			out[s] = c
		}
	}
	return out
}
