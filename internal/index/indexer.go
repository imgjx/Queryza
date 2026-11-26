package index

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"strings"
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
	// writeMu 串行化单篇回落写入：单篇文档会锁上千个 term 行，
	// 并发事务极易交叉死锁。常规路径都走批量引擎，这里只做兜底。
	writeMu sync.Mutex
	// batch 攒批写入引擎：把每篇一次的 redo fsync 摊到几十篇上
	batch *batcher
	// mem 内存倒排，写入成功后同步增量，检索立即可见
	mem *MemIndex
}

// SetMemIndex 挂载内存倒排索引（可为 nil）。
func (ix *Indexer) SetMemIndex(m *MemIndex) { ix.mem = m }

func NewIndexer(db *store.DB, cache *store.Cache, limits config.LimitsConfig, embedder *Embedder) *Indexer {
	ix := &Indexer{db: db, cache: cache, limits: limits, embedder: embedder}
	ix.batch = newBatcher(ix)
	return ix
}

func (ix *Indexer) Embedder() *Embedder { return ix.embedder }

// IndexDocument 写入正排、倒排、词项统计，并刷新 Redis 热数据。
// 常规路径交给批量引擎（40ms 窗口 / 64 篇一提交），单篇自动回落带重试。
func (ix *Indexer) IndexDocument(ctx context.Context, doc *store.Document, tokens []string) error {
	var err error
	if ix.batch != nil {
		err = ix.batch.submit(ctx, doc, tokens)
	} else {
		err = ix.indexSingle(ctx, doc, tokens)
	}
	if err != nil {
		return err
	}
	ix.afterIndex(ctx, doc, tokens)
	return nil
}

// indexSingle 单篇写入 + 有限重试（死锁 1213 / 锁等待 1205）。
func (ix *Indexer) indexSingle(ctx context.Context, doc *store.Document, tokens []string) error {
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
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
		ON DUPLICATE KEY UPDATE
		 title=VALUES(title),content=VALUES(content),content_len=VALUES(content_len),
		 http_status=VALUES(http_status),fetched_at=VALUES(fetched_at),content_hash=VALUES(content_hash),
		 simhash=VALUES(simhash),site_weight=VALUES(site_weight),index_status=1,id=LAST_INSERT_ID(id)`,
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

	// 按 term 字典序写入，固定加锁顺序，避免并发事务交叉等待
	terms := make([]string, 0, len(tf))
	for t := range tf {
		// term 列宽 80，超长词直接丢弃，避免整篇索引失败
		if len([]rune(t)) > 80 {
			continue
		}
		terms = append(terms, t)
	}
	sort.Strings(terms)

	// 批量多值写入：原实现每个词两次独立 INSERT，一篇 600 词要 1200 次往返，
	// 现在按 400 行一批，往返次数降到个位数。
	const chunk = 400
	for start := 0; start < len(terms); start += chunk {
		end := start + chunk
		if end > len(terms) {
			end = len(terms)
		}
		var sb strings.Builder
		sb.Grow((end - start) * 20)
		sb.WriteString(`INSERT IGNORE INTO postings(term,doc_id,tf) VALUES `)
		args := make([]interface{}, 0, (end-start)*3)
		for i := start; i < end; i++ {
			if i > start {
				sb.WriteByte(',')
			}
			sb.WriteString(`(?,?,?)`)
			args = append(args, terms[i], id, tf[terms[i]])
		}
		if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
			return err
		}

		var tb strings.Builder
		tb.Grow((end - start) * 16)
		tb.WriteString(`INSERT INTO terms(term,df) VALUES `)
		targs := make([]interface{}, 0, (end-start)*2)
		for i := start; i < end; i++ {
			if i > start {
				tb.WriteByte(',')
			}
			tb.WriteString(`(?,?)`)
			targs = append(targs, terms[i], tf[terms[i]])
		}
		tb.WriteString(` ON DUPLICATE KEY UPDATE df=df+VALUES(df)`)
		if _, err := tx.ExecContext(ctx, tb.String(), targs...); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	doc.IndexStatus = 1
	return nil
}

// afterIndex 刷热榜。原来这里还会把整篇文档（含正文）JSON 序列化写进 Redis，
// 但没有任何读取方，纯属浪费带宽与 CPU，已移除。
func (ix *Indexer) afterIndex(ctx context.Context, doc *store.Document, tokens []string) {
	if ix.mem != nil && doc.ID != 0 && len(tokens) > 0 {
		ix.mem.Add(doc.ID, &DocMeta{
			DL: int32(doc.ContentLen), Site: float32(doc.SiteWeight), Dyn: float32(doc.DynamicWeight),
			Trust: float32(doc.TrustScore), Spam: float32(doc.SpamScore), Fetched: doc.FetchedAt.Unix(),
		}, tokens)
	}
	if ix.cache == nil || doc.ID == 0 {
		return
	}
	ix.cache.ZAdd(ctx, "qz:hot:docs", redis.Z{Score: redisScore(time.Now()), Member: itoa(doc.ID)})
	// 裁剪低频调用，避免每篇一次往返
	if doc.ID%64 == 0 {
		ix.cache.ZRemRangeByRank(ctx, "qz:hot:docs", 0, int64(-ix.limits.MaxRedisDocs-1))
	}
	if ix.embedder != nil && ix.embedder.Enabled() {
		go ix.embedAsync(doc.ID, doc.Title+" "+doc.Content)
	}
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
