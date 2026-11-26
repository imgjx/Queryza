package index

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"

	"queryza/internal/config"
	"queryza/internal/store"
)

type Indexer struct {
	db       *store.DB
	cache    *store.Cache
	limits   config.LimitsConfig
	embedder *Embedder
	// writeMu 串行化索引写入：单篇文档会锁上千个 term 行，
	// 并发事务极易交叉死锁，串行写入反而更稳且吞吐足够
	writeMu sync.Mutex
}

func NewIndexer(db *store.DB, cache *store.Cache, limits config.LimitsConfig, embedder *Embedder) *Indexer {
	return &Indexer{db: db, cache: cache, limits: limits, embedder: embedder}
}

func (ix *Indexer) Embedder() *Embedder { return ix.embedder }

// IndexDocument 写入正排、倒排、词项统计，并刷新 Redis 热数据。
// 并发写入时多个事务会对 terms 行交叉加锁，偶发 1213 死锁，这里做有限重试。
func (ix *Indexer) IndexDocument(ctx context.Context, doc *store.Document, tokens []string) error {
	ix.writeMu.Lock()
	defer ix.writeMu.Unlock()

	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(15*attempt) * time.Millisecond):
			}
		}
		err := ix.indexOnce(ctx, doc, tokens)
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryable(err) {
			return err
		}
	}
	return lastErr
}

// retryable 判断是否为死锁 / 锁等待超时
func retryable(err error) bool {
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		return me.Number == 1213 || me.Number == 1205
	}
	return false
}

func (ix *Indexer) indexOnce(ctx context.Context, doc *store.Document, tokens []string) error {
	tf := make(map[string]int, len(tokens))
	for _, t := range tokens {
		if t != "" {
			tf[t]++
		}
	}
	if len(tf) == 0 {
		return nil
	}

	tx, err := ix.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `INSERT INTO documents
		(url,url_hash,domain,title,content,content_len,http_status,fetched_at,content_hash,simhash,
		 site_weight,dynamic_weight,trust_score,spam_score,is_hot,index_status)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)
		ON DUPLICATE KEY UPDATE
		 title=VALUES(title),content=VALUES(content),content_len=VALUES(content_len),
		 http_status=VALUES(http_status),fetched_at=VALUES(fetched_at),content_hash=VALUES(content_hash),
		 simhash=VALUES(simhash),site_weight=VALUES(site_weight),index_status=0,id=LAST_INSERT_ID(id)`,
		doc.URL, doc.URLHash, doc.Domain, doc.Title, doc.Content, doc.ContentLen, doc.HTTPStatus,
		doc.FetchedAt, doc.ContentHash, doc.SimHash, doc.SiteWeight, doc.DynamicWeight, doc.TrustScore, doc.SpamScore, doc.IsHot)
	if err != nil {
		return err
	}
	id, _ := res.LastInsertId()
	doc.ID = id

	if _, err := tx.ExecContext(ctx, `DELETE FROM postings WHERE doc_id=?`, id); err != nil {
		return err
	}
	ps, err := tx.PrepareContext(ctx, `INSERT IGNORE INTO postings(term,doc_id,tf) VALUES(?,?,?)`)
	if err != nil {
		return err
	}
	ts, err := tx.PrepareContext(ctx, `INSERT INTO terms(term,df) VALUES(?,1) ON DUPLICATE KEY UPDATE df=df+1`)
	if err != nil {
		ps.Close()
		return err
	}
	// 固定加锁顺序：按 term 字典序写入，避免并发事务交叉等待形成死锁
	terms := make([]string, 0, len(tf))
	for t := range tf {
		terms = append(terms, t)
	}
	sort.Strings(terms)
	for _, term := range terms {
		// term 列宽 80，超长词直接丢弃，避免整篇索引失败
		if len([]rune(term)) > 80 {
			continue
		}
		if _, err := ps.ExecContext(ctx, term, id, tf[term]); err != nil {
			ps.Close()
			ts.Close()
			return err
		}
		if _, err := ts.ExecContext(ctx, term); err != nil {
			ps.Close()
			ts.Close()
			return err
		}
	}
	ps.Close()
	ts.Close()

	if _, err := tx.ExecContext(ctx, `UPDATE documents SET index_status=1 WHERE id=?`, id); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	doc.IndexStatus = 1

	// Redis 热数据：缓存文档快照 + 维护热榜
	if ix.cache != nil {
		if b, err := json.Marshal(doc); err == nil {
			ix.cache.SetJSON(ctx, "qz:doc:"+itoa(id), b, 7*24*time.Hour)
		}
		ix.cache.ZAdd(ctx, "qz:hot:docs", redis.Z{Score: redisScore(time.Now()), Member: itoa(id)})
		ix.cache.ZRemRangeByRank(ctx, "qz:hot:docs", 0, int64(-ix.limits.MaxRedisDocs-1))
	}

	// 异步生成向量
	if ix.embedder != nil && ix.embedder.Enabled() {
		go ix.embedAsync(id, doc.Title+" "+doc.Content)
	}
	return nil
}

func (ix *Indexer) embedAsync(id int64, text string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	vec, err := ix.embedder.Embed(ctx, text)
	if err != nil {
		return
	}
	b := FloatsToBytes(vec)
	ix.db.ExecContext(ctx, `UPDATE documents SET embedding=?, embedding_dim=? WHERE id=?`, b, len(vec), id)
}

func FloatToBytes(f float32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, math.Float32bits(f))
	return b
}

func FloatsToBytes(v []float32) []byte {
	b := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(f))
	}
	return b
}

func BytesToFloats(b []byte) []float32 {
	n := len(b) / 4
	v := make([]float32, n)
	for i := 0; i < n; i++ {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:]))
	}
	return v
}

func redisScore(t time.Time) float64 { return float64(t.Unix()) }

func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
