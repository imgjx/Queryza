package index

import (
	"context"
	"sort"
	"strings"
	"time"

	"queryza/internal/store"
)

// 批量索引：单篇一个事务时，每次提交都要一次 redo fsync，
// 加上 documents 大字段插入，单篇固定成本约 7ms。
// 攒批提交把这些固定成本摊到几十篇上，写入吞吐提升约一个量级。

type batchReq struct {
	doc    *store.Document
	tokens []string
	done   chan error
}

const (
	batchMax     = 64
	batchWindow  = 2 * time.Millisecond
	batchQueueSz = 4096
)

type batcher struct {
	ix  *Indexer
	ch  chan *batchReq
	ctx context.Context
}

func newBatcher(ix *Indexer) *batcher {
	b := &batcher{ix: ix, ch: make(chan *batchReq, batchQueueSz), ctx: context.Background()}
	go b.loop()
	return b
}

func (b *batcher) submit(ctx context.Context, doc *store.Document, tokens []string) error {
	req := &batchReq{doc: doc, tokens: tokens, done: make(chan error, 1)}
	select {
	case b.ch <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *batcher) loop() {
	for {
		req, ok := <-b.ch
		if !ok {
			return
		}
		pending := make([]*batchReq, 0, batchMax)
		pending = append(pending, req)

		// 先把已经排队的请求全部吸进来（无额外等待）
	drain:
		for len(pending) < batchMax {
			select {
			case r, ok := <-b.ch:
				if !ok {
					b.flush(pending)
					return
				}
				pending = append(pending, r)
			default:
				break drain
			}
		}

		// 并发度不高时给一点聚集时间，把 fsync 摊薄；串行场景最多多等 2ms
		if len(pending) < 8 {
			timer := time.NewTimer(batchWindow)
		collect:
			for len(pending) < 8 {
				select {
				case r, ok := <-b.ch:
					if !ok {
						timer.Stop()
						b.flush(pending)
						return
					}
					pending = append(pending, r)
				case <-timer.C:
					break collect
				}
			}
			timer.Stop()
		}
		b.flush(pending)
	}
}

func (b *batcher) flush(reqs []*batchReq) {
	if len(reqs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	err := b.ix.indexBatch(ctx, reqs)
	cancel()
	if err == nil {
		for _, r := range reqs {
			b.ix.afterIndex(b.ctx, r.doc, r.tokens)
			r.done <- nil
		}
		return
	}
	// 整批失败时逐篇回落，避免个别脏数据拖垮整批
	for _, r := range reqs {
		c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		e := b.ix.indexOnce(c, r.doc, r.tokens)
		cancel()
		r.done <- e
	}
}

type prepared struct {
	doc  *store.Document
	tf   map[string]int
	terms []string
	id   int64
}

func (b *batcher) ix0() *Indexer { return b.ix }

func (ix *Indexer) indexBatch(ctx context.Context, reqs []*batchReq) error {
	ps := make([]*prepared, 0, len(reqs))
	for _, r := range reqs {
		tf := make(map[string]int, len(r.tokens))
		for _, t := range r.tokens {
			if t != "" {
				tf[t]++
			}
		}
		if len(tf) == 0 {
			continue
		}
		terms := make([]string, 0, len(tf))
		for t := range tf {
			if len([]rune(t)) > 80 {
				continue
			}
			terms = append(terms, t)
		}
		sort.Strings(terms)
		ps = append(ps, &prepared{doc: r.doc, tf: tf, terms: terms})
	}
	if len(ps) == 0 {
		return nil
	}

	tx, err := ix.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1) 正排
	for _, p := range ps {
		d := p.doc
		res, err := tx.ExecContext(ctx, `INSERT INTO documents
			(url,url_hash,domain,title,content,content_len,http_status,fetched_at,content_hash,simhash,
			 site_weight,dynamic_weight,trust_score,spam_score,is_hot,index_status)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
			ON DUPLICATE KEY UPDATE
			 title=VALUES(title),content=VALUES(content),content_len=VALUES(content_len),
			 http_status=VALUES(http_status),fetched_at=VALUES(fetched_at),content_hash=VALUES(content_hash),
			 simhash=VALUES(simhash),site_weight=VALUES(site_weight),index_status=1,id=LAST_INSERT_ID(id)`,
			d.URL, d.URLHash, d.Domain, d.Title, d.Content, d.ContentLen, d.HTTPStatus,
			d.FetchedAt, d.ContentHash, d.SimHash, d.SiteWeight, d.DynamicWeight, d.TrustScore, d.SpamScore, d.IsHot)
		if err != nil {
			return err
		}
		p.id, _ = res.LastInsertId()
		d.ID = p.id
	}

	// 2) 清掉旧倒排（URL 更新时）
	var db strings.Builder
	db.WriteString(`DELETE FROM postings WHERE doc_id IN (`)
	dargs := make([]interface{}, 0, len(ps))
	for i, p := range ps {
		if i > 0 {
			db.WriteByte(',')
		}
		db.WriteByte('?')
		dargs = append(dargs, p.id)
	}
	db.WriteByte(')')
	if _, err := tx.ExecContext(ctx, db.String(), dargs...); err != nil {
		return err
	}

	// 3) 倒排：所有篇目的 posting 合并后按 term 字典序写入，
	//    既是顺序写，也彻底避开并发事务的行锁交叉
	type row struct {
		term string
		id   int64
		tf   int
	}
	total := 0
	for _, p := range ps {
		total += len(p.terms)
	}
	rows := make([]row, 0, total)
	for _, p := range ps {
		for _, t := range p.terms {
			rows = append(rows, row{term: t, id: p.id, tf: p.tf[t]})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].term != rows[j].term {
			return rows[i].term < rows[j].term
		}
		return rows[i].id < rows[j].id
	})

	const chunk = 800
	for start := 0; start < len(rows); start += chunk {
		end := start + chunk
		if end > len(rows) {
			end = len(rows)
		}
		var sb strings.Builder
		sb.Grow((end - start) * 18)
		sb.WriteString(`INSERT IGNORE INTO postings(term,doc_id,tf) VALUES `)
		args := make([]interface{}, 0, (end-start)*3)
		for i := start; i < end; i++ {
			if i > start {
				sb.WriteByte(',')
			}
			sb.WriteString(`(?,?,?)`)
			args = append(args, rows[i].term, rows[i].id, rows[i].tf)
		}
		if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
			return err
		}
	}

	// 4) 词项 df：同批内先聚合，再一次性 upsert
	df := make(map[string]int, len(rows)/2)
	for _, r := range rows {
		df[r.term]++
	}
	dfTerms := make([]string, 0, len(df))
	for t := range df {
		dfTerms = append(dfTerms, t)
	}
	sort.Strings(dfTerms)
	for start := 0; start < len(dfTerms); start += chunk {
		end := start + chunk
		if end > len(dfTerms) {
			end = len(dfTerms)
		}
		var tb strings.Builder
		tb.Grow((end - start) * 14)
		tb.WriteString(`INSERT INTO terms(term,df) VALUES `)
		targs := make([]interface{}, 0, (end-start)*2)
		for i := start; i < end; i++ {
			if i > start {
				tb.WriteByte(',')
			}
			tb.WriteString(`(?,?)`)
			targs = append(targs, dfTerms[i], df[dfTerms[i]])
		}
		tb.WriteString(` ON DUPLICATE KEY UPDATE df=df+VALUES(df)`)
		if _, err := tx.ExecContext(ctx, tb.String(), targs...); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	for _, p := range ps {
		p.doc.IndexStatus = 1
	}
	return nil
}
