package crawler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"time"

	"queryza/internal/store"
)

type Task struct {
	ID       int64
	URL      string
	URLHash  string
	Domain   string
	Depth    int
	Priority int
}

type Frontier struct {
	db      *store.DB
	nodeID  string
	max     int
	domains []string // 本节点负责的域名，非空时只领取这些域名的任务
}

func NewFrontier(db *store.DB, nodeID string, max int) *Frontier {
	if max <= 0 {
		max = 500000
	}
	return &Frontier{db: db, nodeID: nodeID, max: max}
}

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
	var cnt int64
	f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM crawl_queue`).Scan(&cnt)
	room := f.max - int(cnt)
	if room <= 0 {
		return 0, nil
	}
	added := 0
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT IGNORE INTO crawl_queue
		(url,url_hash,domain,depth,priority,status,node_id) VALUES(?,?,?,?,?,'pending','')`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	for _, t := range tasks {
		if added >= room {
			break
		}
		if t.URLHash == "" {
			t.URLHash = URLHash(t.URL)
		}
		r, err := stmt.ExecContext(ctx, t.URL, t.URLHash, t.Domain, t.Depth, t.Priority)
		if err != nil {
			continue
		}
		if n, _ := r.RowsAffected(); n > 0 {
			added++
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return added, nil
}

// SetDomains 限定本节点可领取的域名；传 nil 表示不限制。
func (f *Frontier) SetDomains(ds []string) { f.domains = ds }

// Lease 原子领取任务。
func (f *Frontier) Lease(ctx context.Context, n int) ([]Task, error) {
	if n <= 0 {
		n = 1
	}
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	q := `SELECT id,url,url_hash,domain,depth,priority FROM crawl_queue WHERE status='pending'`
	var args []interface{}
	if len(f.domains) > 0 && len(f.domains) <= 800 {
		q += ` AND domain IN (` + placeholders(len(f.domains)) + `)`
		for _, d := range f.domains {
			args = append(args, d)
		}
	}
	q += ` ORDER BY priority DESC, id ASC LIMIT ? FOR UPDATE SKIP LOCKED`
	args = append(args, n)

	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	var tasks []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.URL, &t.URLHash, &t.Domain, &t.Depth, &t.Priority); err == nil {
			tasks = append(tasks, t)
		}
	}
	rows.Close()
	if len(tasks) == 0 {
		return nil, nil
	}
	stmt, _ := tx.PrepareContext(ctx, `UPDATE crawl_queue SET status='fetching', node_id=?, updated_at=NOW() WHERE id=?`)
	for _, t := range tasks {
		stmt.ExecContext(ctx, f.nodeID, t.ID)
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return tasks, nil
}

// Release 把任务放回队列（非本节点负责的域名用）
func (f *Frontier) Release(ctx context.Context, id int64) {
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
	f.db.ExecContext(ctx, `UPDATE crawl_queue SET status='done', updated_at=NOW() WHERE id=?`, id)
}

func (f *Frontier) Fail(ctx context.Context, id int64, retries int) {
	if retries >= 3 {
		f.db.ExecContext(ctx, `UPDATE crawl_queue SET status='error', retries=?, updated_at=NOW() WHERE id=?`, retries, id)
		return
	}
	f.db.ExecContext(ctx, `UPDATE crawl_queue SET status='pending', retries=?, updated_at=NOW() WHERE id=?`, retries, id)
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
