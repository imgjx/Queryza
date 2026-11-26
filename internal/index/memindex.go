package index

import (
	"context"
	"log"
	"sync"
	"time"

	"queryza/internal/store"
)

// MemIndex 内存倒排索引：把 terms/postings/文档元数据常驻内存，
// 检索不再走 MySQL 的 JOIN 全表扫描。检索耗时从百毫秒级降到毫秒级。
//
// 规模上限由 maxDocs 控制；超限时自动放弃内存索引，检索回落到 SQL 路径，
// 因此不会在小内存机器上被撑爆。

type Posting struct {
	DocID int64
	TF    int32
}

type DocMeta struct {
	DL      int32
	Site    float32
	Dyn     float32
	Trust   float32
	Spam    float32
	Fetched int64 // Unix 秒
}

type MemIndex struct {
	mu       sync.RWMutex
	docs     map[int64]*DocMeta
	postings map[string][]Posting
	n        int
	sumDL    int64
	maxDocs  int

	ready   bool   // 已完成首次加载
	enabled bool   // 未超限、可用
	loading bool   //
	built   time.Time

	// 加载期间写入的文档先挂在 pending，加载完成后补进去，避免检索漏数据
	pending []memDoc
}

type memDoc struct {
	id     int64
	meta   *DocMeta
	tf     map[string]int
	tokens []string
}

func NewMemIndex(maxDocs int) *MemIndex {
	if maxDocs <= 0 {
		maxDocs = 200000
	}
	return &MemIndex{
		docs:     map[int64]*DocMeta{},
		postings: map[string][]Posting{},
		maxDocs:  maxDocs,
	}
}

func (m *MemIndex) Ready() bool { return m != nil && m.ready && m.enabled }

func (m *MemIndex) Stats() map[string]interface{} {
	if m == nil {
		return map[string]interface{}{"enabled": false}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return map[string]interface{}{
		"enabled":   m.ready && m.enabled,
		"docs":      len(m.docs),
		"terms":     len(m.postings),
		"max_docs":  m.maxDocs,
		"loaded_at": m.built,
	}
}

// Build 从数据库全量加载。后台调用，不阻塞服务启动。
func (m *MemIndex) Build(db *store.DB) {
	m.mu.Lock()
	if m.loading || m.ready {
		m.mu.Unlock()
		return
	}
	m.loading = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.loading = false
		m.mu.Unlock()
	}()

	start := time.Now()
	ctx := context.Background()

	var total int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents WHERE index_status=1`).Scan(&total); err != nil {
		log.Printf("内存索引: 统计文档失败 %v", err)
		return
	}
	if total > m.maxDocs {
		log.Printf("内存索引: 文档数 %d 超过上限 %d，跳过内存索引，检索走 SQL", total, m.maxDocs)
		m.mu.Lock()
		m.ready, m.enabled = true, false
		m.mu.Unlock()
		return
	}

	// 1) 文档元数据
	rows, err := db.QueryContext(ctx, `SELECT id,content_len,site_weight,dynamic_weight,
		trust_score,spam_score,UNIX_TIMESTAMP(fetched_at) FROM documents WHERE index_status=1`)
	if err != nil {
		log.Printf("内存索引: 读文档失败 %v", err)
		return
	}
	docs := make(map[int64]*DocMeta, total)
	var sumDL int64
	for rows.Next() {
		var id int64
		var dl int32
		var site, dyn, trust, spam float32
		var ft int64
		if rows.Scan(&id, &dl, &site, &dyn, &trust, &spam, &ft) == nil {
			docs[id] = &DocMeta{DL: dl, Site: site, Dyn: dyn, Trust: trust, Spam: spam, Fetched: ft}
			sumDL += int64(dl)
		}
	}
	rows.Close()

	// 2) 倒排
	post := make(map[string][]Posting, 1<<18)
	prows, err := db.QueryContext(ctx, `SELECT term,doc_id,tf FROM postings`)
	if err != nil {
		log.Printf("内存索引: 读倒排失败 %v", err)
		return
	}
	lastTerm := ""
	var cur []Posting
	for prows.Next() {
		var term string
		var docID int64
		var tf int32
		if prows.Scan(&term, &docID, &tf) != nil {
			continue
		}
		// 按 term 顺序读（主键就是 term,doc_id），批量落 map 降低哈希开销
		if term != lastTerm {
			if lastTerm != "" {
				post[lastTerm] = cur
			}
			lastTerm = term
			cur = make([]Posting, 0, 8)
		}
		cur = append(cur, Posting{DocID: docID, TF: tf})
	}
	if lastTerm != "" {
		post[lastTerm] = cur
	}
	prows.Close()

	m.mu.Lock()
	m.docs = docs
	m.postings = post
	m.n = len(docs)
	m.sumDL = sumDL
	m.ready, m.enabled = true, true
	m.built = time.Now()
	pend := m.pending
	m.pending = nil
	m.mu.Unlock()

	for _, d := range pend {
		m.apply(d.id, d.meta, d.tokens)
	}
	log.Printf("内存索引: 加载完成 %d 篇 / %d 词项，耗时 %s", len(docs), len(post), time.Since(start).Round(time.Millisecond))
}

// Add 增量写入一篇文档（索引层写入成功后调用）。
func (m *MemIndex) Add(id int64, meta *DocMeta, tokens []string) {
	if m == nil || id == 0 {
		return
	}
	m.mu.Lock()
	if !m.ready {
		// 加载中：先挂起，加载完成后回放
		m.pending = append(m.pending, memDoc{id: id, meta: meta, tokens: tokens})
		m.mu.Unlock()
		return
	}
	if !m.enabled {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	m.apply(id, meta, tokens)
}

func (m *MemIndex) apply(id int64, meta *DocMeta, tokens []string) {
	tf := make(map[string]int, len(tokens))
	for _, t := range tokens {
		if t != "" {
			tf[t]++
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.docs[id]; ok {
		m.sumDL -= int64(old.DL)
	}
	m.docs[id] = meta
	m.sumDL += int64(meta.DL)
	m.n = len(m.docs)
	for t, c := range tf {
		m.postings[t] = append(m.postings[t], Posting{DocID: id, TF: int32(c)})
	}
}

// SearchHit 内存检索的中间结果
type MemHit struct {
	DocID int64
	Meta  *DocMeta
	BM25  float64
}

// Stats2 返回 N 与平均文档长度
func (m *MemIndex) Size() (int, float64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	avg := 1.0
	if m.n > 0 && m.sumDL > 0 {
		avg = float64(m.sumDL) / float64(m.n)
	}
	return m.n, avg
}

// PostingsOf 返回词项倒排（只读，调用方不得修改）。
func (m *MemIndex) PostingsOf(term string) []Posting {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.postings[term]
}

// MetaOf 取单篇元数据（只读）。
func (m *MemIndex) MetaOf(id int64) *DocMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.docs[id]
}

// DocsByID 批量取元数据。
func (m *MemIndex) DocsByID(ids []int64) map[int64]*DocMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[int64]*DocMeta, len(ids))
	for _, id := range ids {
		if d, ok := m.docs[id]; ok {
			out[id] = d
		}
	}
	return out
}
