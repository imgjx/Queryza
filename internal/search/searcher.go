package search

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"queryza/internal/algo"
	"queryza/internal/config"
	"queryza/internal/index"
	"queryza/internal/store"
)

type Searcher struct {
	db       *store.DB
	cache    *store.Cache
	cfg      config.SearchConfig
	embedder *index.Embedder
	nodeID   string
	mem      *index.MemIndex
}

func New(db *store.DB, cache *store.Cache, cfg config.SearchConfig, embedder *index.Embedder, nodeID string) *Searcher {
	return &Searcher{db: db, cache: cache, cfg: cfg, embedder: embedder, nodeID: nodeID}
}

// SetMemIndex 挂载内存倒排索引；就绪后检索走内存，否则回落 SQL。
func (s *Searcher) SetMemIndex(m *index.MemIndex) { s.mem = m }

type Result struct {
	Query  string            `json:"query"`
	Tokens []string          `json:"tokens"`
	Mode   string            `json:"mode"`
	Total  int               `json:"total"`
	TookMS int               `json:"took_ms"`
	Hits   []store.SearchHit `json:"hits"`
	Cached bool              `json:"cached"`
	NodeID string            `json:"node_id"`
	// 分页元信息：page 从 1 开始，pages 为总页数（total 为真实召回量）
	Page  int `json:"page"`
	Size  int `json:"size"`
	Pages int `json:"pages"`
	// Degraded 记录本次检索失败的节点，便于观察集群健康
	Degraded []string `json:"degraded,omitempty"`
}

// Paginate 按 page/size 设置分页元信息（不修改 Hits）。
func (r *Result) Paginate(page, size int) {
	if size <= 0 {
		size = 10
	}
	if page <= 0 {
		page = 1
	}
	r.Page, r.Size = page, size
	if r.Total <= 0 {
		r.Pages = 0
	} else {
		r.Pages = (r.Total + size - 1) / size
	}
}

func (s *Searcher) SearchLocal(ctx context.Context, query string, topK int) (*Result, error) {
	return s.search(ctx, query, topK, nil)
}

// SearchLocalWithin 只在本节点负责的域名内检索。
// allow 为 nil 表示不限制；空 map 表示一个域名都不属于本节点，直接返回空。
func (s *Searcher) SearchLocalWithin(ctx context.Context, query string, topK int, allow map[string]bool) (*Result, error) {
	if allow != nil && len(allow) == 0 {
		return &Result{Query: query, Hits: []store.SearchHit{}, NodeID: s.nodeID}, nil
	}
	return s.search(ctx, query, topK, allow)
}

func (s *Searcher) search(ctx context.Context, query string, topK int, allow map[string]bool) (*Result, error) {
	start := time.Now()
	query = strings.TrimSpace(query)
	if query == "" {
		return &Result{Query: query, Hits: []store.SearchHit{}}, nil
	}
	if topK <= 0 {
		topK = s.cfg.TopK
	}

	ck := "qz:q:" + hashHex(s.nodeID+"|"+query+"|"+fmt.Sprint(topK))
	// 带域名过滤时结果依赖副本分配，不走共享缓存
	if s.cache != nil && allow == nil {
		if b, ok := s.cache.GetJSON(ctx, ck); ok {
			var r Result
			if json.Unmarshal(b, &r) == nil {
				r.Cached = true
				return &r, nil
			}
		}
	}

	tokens := algo.Tokenize(query)
	if len(tokens) == 0 {
		tokens = []string{strings.ToLower(query)}
	}

	// 内存索引就绪时走内存检索（毫秒级），否则回落 SQL
	if s.mem != nil && s.mem.Ready() {
		return s.searchMem(ctx, query, tokens, topK, allow, ck, start)
	}

	var N int
	var avgdl float64
	s.db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(AVG(content_len),1) FROM documents WHERE index_status=1`).Scan(&N, &avgdl)
	if avgdl <= 0 {
		avgdl = 1
	}

	kw := map[int64]float64{}
	if N > 0 {
		ph := placeholders(len(tokens))
		args := toArgs(tokens)
		dfs := map[string]int{}
		if rows, err := s.db.QueryContext(ctx, `SELECT term,df FROM terms WHERE term IN (`+ph+`)`, args...); err == nil {
			for rows.Next() {
				var t string
				var d int
				if rows.Scan(&t, &d) == nil {
					dfs[t] = d
				}
			}
			rows.Close()
		}
		if rows, err := s.db.QueryContext(ctx, `SELECT p.doc_id,p.term,p.tf,d.content_len
			FROM postings p JOIN documents d ON d.id=p.doc_id
			WHERE p.term IN (`+ph+`)`, args...); err == nil {
			for rows.Next() {
				var did int64
				var term string
				var tf, dl int
				if rows.Scan(&did, &term, &tf, &dl) == nil {
					df := dfs[term]
					if df <= 0 {
						df = 1
					}
					kw[did] += algo.BM25(tf, df, dl, avgdl, N, s.cfg.BM25K1, s.cfg.BM25B)
				}
			}
			rows.Close()
		}
	}

	vec := map[int64]float64{}
	mode := "keyword"
	if s.cfg.Hybrid && s.embedder != nil && s.embedder.Enabled() {
		if qv, err := s.embedder.Embed(ctx, query); err == nil && len(qv) > 0 {
			mode = "hybrid"
			rows, err := s.db.QueryContext(ctx, `SELECT id,embedding FROM documents
				WHERE embedding IS NOT NULL AND embedding_dim=? LIMIT 5000`, len(qv))
			if err == nil {
				var ids []int64
				var docs [][]float32
				for rows.Next() {
					var id int64
					var b []byte
					if rows.Scan(&id, &b) == nil {
						ids = append(ids, id)
						docs = append(docs, index.BytesToFloats(b))
					}
				}
				rows.Close()
				if len(docs) > 0 {
					sims := algo.CosineBatch(qv, docs, len(qv))
					for i, id := range ids {
						vec[id] = sims[i]
					}
				}
			}
		}
	}

	kwN := NormalizeMap(kw)
	vecN := NormalizeMap(vec)
	cand := map[int64]float64{}
	for id, v := range kwN {
		cand[id] += v * s.cfg.KeywordWeight
	}
	for id, v := range vecN {
		cand[id] += v * s.cfg.VectorWeight
	}
	if len(cand) == 0 {
		r := &Result{Query: query, Tokens: tokens, Mode: mode, Total: 0, TookMS: int(time.Since(start).Milliseconds()), Hits: []store.SearchHit{}, NodeID: s.nodeID}
		return r, nil
	}

	limit := topK
	if s.cfg.RerankTopN > limit {
		limit = s.cfg.RerankTopN
	}
	if limit > 500 {
		limit = 500
	}
	ids := topN(cand, limit)

	hits, err := s.loadHits(ctx, ids, cand, kw, vec, tokens, allow)
	if err != nil {
		return nil, err
	}

	sort.SliceStable(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
	total := len(cand)
	if len(hits) > topK {
		hits = hits[:topK]
	}

	res := &Result{Query: query, Tokens: tokens, Mode: mode, Total: total, TookMS: int(time.Since(start).Milliseconds()), Hits: hits, NodeID: s.nodeID}
	if s.cache != nil && allow == nil {
		if b, err := json.Marshal(res); err == nil {
			s.cache.SetJSON(ctx, ck, b, 60*time.Second)
		}
		s.cache.IncrHot(ctx, query)
	}
	return res, nil
}

func (s *Searcher) loadHits(ctx context.Context, ids []int64, cand, kw, vec map[int64]float64, tokens []string, allow map[string]bool) ([]store.SearchHit, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	ph := placeholders(len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,url,title,domain,LEFT(content,6000),fetched_at,
		site_weight,dynamic_weight,trust_score,spam_score
		FROM documents WHERE id IN (`+ph+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hits []store.SearchHit
	for rows.Next() {
		var id int64
		var url, title, domain, content string
		var fetched time.Time
		var site, dyn, trust, spam float64
		if err := rows.Scan(&id, &url, &title, &domain, &content, &fetched, &site, &dyn, &trust, &spam); err != nil {
			continue
		}
		if allow != nil && !allow[domain] {
			continue
		}
		age := time.Since(fetched).Hours() / 24
		fresh := math.Exp(-age / 30)
		score := cand[id] + s.cfg.SiteWeightW*site + s.cfg.DynamicWeightW*dyn +
			s.cfg.TrustWeightW*trust - s.cfg.SpamPenaltyW*spam + s.cfg.FreshnessW*fresh

		src := "keyword"
		if _, ok := vec[id]; ok && s.cfg.Hybrid {
			if _, ok2 := kw[id]; ok2 {
				src = "hybrid"
			} else {
				src = "vector"
			}
		}
		hits = append(hits, store.SearchHit{
			DocID: id, URL: url, Title: title, Domain: domain,
			Snippet: snippet(content, tokens, s.cfg.SnippetLen),
			Score: round4(score), BM25: round4(kw[id]), Vector: round4(vec[id]),
			SiteWeight: site, FetchedAt: fetched.Format("2006-01-02 15:04"), Source: src,
		})
	}
	return hits, nil
}

func snippet(content string, tokens []string, n int) string {
	if n <= 0 {
		n = 160
	}
	rs := []rune(content)
	if len(rs) <= n {
		return content
	}
	pos := -1
	for _, t := range tokens {
		if idx := strings.Index(content, t); idx >= 0 {
			pos = len([]rune(content[:idx]))
			break
		}
	}
	start := 0
	if pos > n/3 {
		start = pos - n/3
	}
	if start+n > len(rs) {
		start = len(rs) - n
	}
	if start < 0 {
		start = 0
	}
	return "…" + string(rs[start:start+n]) + "…"
}

func NormalizeMap(m map[int64]float64) map[int64]float64 {
	out := make(map[int64]float64, len(m))
	if len(m) == 0 {
		return out
	}
	mn, mx := math.Inf(1), math.Inf(-1)
	for _, v := range m {
		if v < mn {
			mn = v
		}
		if v > mx {
			mx = v
		}
	}
	span := mx - mn
	for k, v := range m {
		if span <= 1e-12 {
			out[k] = 0
		} else {
			out[k] = (v - mn) / span
		}
	}
	return out
}

func topN(m map[int64]float64, n int) []int64 {
	type kv struct {
		id int64
		v  float64
	}
	arr := make([]kv, 0, len(m))
	for k, v := range m {
		arr = append(arr, kv{k, v})
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i].v > arr[j].v })
	if n > len(arr) {
		n = len(arr)
	}
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		out[i] = arr[i].id
	}
	return out
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func toArgs(ss []string) []interface{} {
	a := make([]interface{}, len(ss))
	for i, s := range ss {
		a[i] = s
	}
	return a
}

func hashHex(s string) string {
	h := sha1.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}

func round4(f float64) float64 { return math.Round(f*10000) / 10000 }
