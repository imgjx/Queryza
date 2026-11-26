package crawler

import (
	"context"
	"strings"
	"sync"
	"time"

	"queryza/internal/store"
)

// batchWriter 把抓取日志与队列状态更新从关键路径上挪走：
// 原先每抓一页要同步写 1 条 crawl_logs + 1 条 crawl_queue UPDATE，
// 24 并发下这些零散写会互相抢连接和行锁，吞吐被拖住。
type batchWriter struct {
	db *store.DB

	logs chan logRec
	qs   chan qRec

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

type logRec struct {
	url, domain, status, errStr, nodeID, ruleID string
	httpCode, bytes, durMS                      int
}

type qRec struct {
	id      int64
	status  string
	retries int
}

func newBatchWriter(db *store.DB) *batchWriter {
	w := &batchWriter{
		db:   db,
		logs: make(chan logRec, 8192),
		qs:   make(chan qRec, 8192),
		stop: make(chan struct{}),
	}
	w.wg.Add(1)
	go w.loop()
	return w
}

func (w *batchWriter) log(r logRec) {
	select {
	case w.logs <- r:
	default: // 队列满时丢弃日志，绝不阻塞抓取
	}
}

func (w *batchWriter) queue(id int64, status string, retries int) {
	select {
	case w.qs <- qRec{id: id, status: status, retries: retries}:
	default:
	}
}

func (w *batchWriter) loop() {
	defer w.wg.Done()
	t := time.NewTicker(300 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-w.stop:
			w.flush()
			return
		case <-t.C:
			w.flush()
		}
	}
}

func (w *batchWriter) flush() {
	w.flushLogs()
	w.flushQueue()
}

func (w *batchWriter) flushLogs() {
	const batch = 400
	for {
		n := len(w.logs)
		if n == 0 {
			return
		}
		if n > batch {
			n = batch
		}
		recs := make([]logRec, 0, n)
		for i := 0; i < n; i++ {
			select {
			case r := <-w.logs:
				recs = append(recs, r)
			default:
			}
		}
		if len(recs) == 0 {
			return
		}
		var sb strings.Builder
		sb.Grow(len(recs) * 48)
		sb.WriteString(`INSERT INTO crawl_logs(url,domain,status,http_code,bytes,error,duration_ms,node_id,rule_id) VALUES `)
		args := make([]interface{}, 0, len(recs)*9)
		for i, r := range recs {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString("(?,?,?,?,?,?,?,?,?)")
			args = append(args, r.url, r.domain, r.status, r.httpCode, r.bytes, r.errStr, r.durMS, r.nodeID, r.ruleID)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, _ = w.db.ExecContext(ctx, sb.String(), args...)
		cancel()
	}
}

func (w *batchWriter) flushQueue() {
	const batch = 500
	groups := map[[2]interface{}][]int64{}
	total := 0
drain:
	for total < batch {
		select {
		case r := <-w.qs:
			k := [2]interface{}{r.status, r.retries}
			groups[k] = append(groups[k], r.id)
			total++
		default:
			break drain
		}
	}
	if total == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for k, ids := range groups {
		var sb strings.Builder
		args := make([]interface{}, 0, len(ids)+2)
		if k[1].(int) < 0 {
			// 不改变重试次数（done / release）
			sb.WriteString(`UPDATE crawl_queue SET status=?, updated_at=NOW() WHERE id IN (`)
			args = append(args, k[0])
		} else {
			sb.WriteString(`UPDATE crawl_queue SET status=?, retries=?, updated_at=NOW() WHERE id IN (`)
			args = append(args, k[0], k[1])
		}
		for i, id := range ids {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteByte('?')
			args = append(args, id)
		}
		sb.WriteByte(')')
		_, _ = w.db.ExecContext(ctx, sb.String(), args...)
	}
}

func (w *batchWriter) Close() {
	w.stopOnce.Do(func() {
		close(w.stop)
		w.wg.Wait()
	})
}
