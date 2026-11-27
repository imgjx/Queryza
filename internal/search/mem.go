package search

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"time"

	"queryza/internal/algo"
	"queryza/internal/index"
	"queryza/internal/store"
)

// searchMem 走内存倒排：BM25 在内存里累加，正文只对少量候选回表取。
func (s *Searcher) searchMem(ctx context.Context, query string, tokens []string, topK int,
	allow map[string]bool, ck string, start time.Time) (*Result, error) {

	N, avgdl := s.mem.Size()
	k1, b := s.cfg.BM25K1, s.cfg.BM25B

	// 词项 df
	type termInfo struct {
		df  int
		idf float64
		auto []index.Posting
	}
	infos := make([]termInfo, 0, len(tokens))
	for _, t := range tokens {
		p := s.mem.PostingsOf(t)
		if len(p) == 0 {
			continue
		}
		df := len(p)
		if df > N {
			df = N
		}
		idf := math.Log(1.0 + (float64(N-df)+0.5)/(float64(df)+0.5))
		infos = append(infos, termInfo{df: df, idf: idf, auto: p})
	}

	mode := "keyword"
	kw := make(map[int64]float64, 4096)
	if len(infos) > 0 {
		for _, ti := range infos {
			for _, p := range ti.auto {
				meta := s.mem.MetaOf(p.DocID)
				if meta == nil {
					continue
				}
				dl := float64(meta.DL)
				if dl <= 0 {
					dl = 1
				}
				tf := float64(p.TF)
				denom := tf + k1*(1.0-b+b*(dl/avgdl))
				if denom > 0 {
					kw[p.DocID] += ti.idf * (tf * (k1 + 1.0)) / denom
				}
			}
		}
	}

	// 向量召回（可选）
	vec := map[int64]float64{}
	if s.cfg.Hybrid && s.embedder != nil && s.embedder.Enabled() {
		if qv, err := s.embedder.Embed(ctx, query); err == nil && len(qv) > 0 {
			mode = "hybrid"
			vec = s.vectorScores(ctx, qv, 5000)
		}
	}

	cand := make(map[int64]float64, len(kw)+len(vec))
	for id, v := range kw {
		cand[id] += v * s.cfg.KeywordWeight
	}
	for id, v := range vec {
		cand[id] += v * s.cfg.VectorWeight
	}
	if len(cand) == 0 {
		return &Result{Query: query, Tokens: tokens, Mode: mode, Total: 0,
			TookMS: int(time.Since(start).Milliseconds()), Hits: []store.SearchHit{}, NodeID: s.nodeID}, nil
	}

	// 只回表取少量候选的正文：站点权重/新鲜度都在内存里，重排不需要太多候选
	limit := topK * 3
	if limit < 40 {
		limit = 40
	}
	if limit > 200 {
		limit = 200
	}
	ids := topN(cand, limit)

	// 候选正文回表：前 900 字足够生成摘要，读得越少越快
	ph := placeholders(len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,url,title,domain,LEFT(content,900),fetched_at
		FROM documents WHERE id IN (`+ph+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	now := time.Now()
	hits := make([]store.SearchHit, 0, len(ids))
	for rows.Next() {
		var id int64
		var url, title, domain, content string
		var fetched time.Time
		if rows.Scan(&id, &url, &title, &domain, &content, &fetched) != nil {
			continue
		}
		if allow != nil && !allow[domain] {
			continue
		}
		meta := s.mem.MetaOf(id)
		var site, dyn, trust, spam float64
		if meta != nil {
			site, dyn, trust, spam = float64(meta.Site), float64(meta.Dyn), float64(meta.Trust), float64(meta.Spam)
		}
		age := now.Sub(fetched).Hours() / 24
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
			Score:   round4(score), BM25: round4(kw[id]), Vector: round4(vec[id]),
			SiteWeight: site, FetchedAt: fetched.Format("2006-01-02 15:04"), Source: src,
		})
	}

	sort.SliceStable(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
	// total 反映真实命中文档数（倒排召回量），不是这一页的条数
	total := len(cand)
	if len(hits) > topK {
		hits = hits[:topK]
	}
	res := &Result{Query: query, Tokens: tokens, Mode: mode, Total: total,
		TookMS: int(time.Since(start).Milliseconds()), Hits: hits, NodeID: s.nodeID}
	if s.cache != nil && allow == nil {
		if b, err := json.Marshal(res); err == nil {
			s.cache.SetJSON(ctx, ck, b, 60*time.Second)
		}
		s.cache.IncrHot(ctx, query)
	}
	return res, nil
}

// vectorScores 拉取有向量的文档并算余弦相似度（仅启用向量检索时使用）。
func (s *Searcher) vectorScores(ctx context.Context, qv []float32, limit int) map[int64]float64 {
	out := map[int64]float64{}
	rows, err := s.db.QueryContext(ctx, `SELECT id,embedding FROM documents
		WHERE embedding IS NOT NULL AND embedding_dim=? LIMIT ?`, len(qv), limit)
	if err != nil {
		return out
	}
	defer rows.Close()
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
	if len(docs) == 0 {
		return out
	}
	sims := algo.CosineBatch(qv, docs, len(qv))
	raw := make(map[int64]float64, len(ids))
	for i, id := range ids {
		raw[id] = sims[i]
	}
	norm := NormalizeMap(raw)
	for k, v := range norm {
		out[k] = v
	}
	return out
}
