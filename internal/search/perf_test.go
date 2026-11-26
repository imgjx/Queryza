package search

import (
	"context"
	"os"
	"testing"

	"queryza/internal/config"
	"queryza/internal/index"
	"queryza/internal/store"
)

func benchSearcher(b *testing.B) *Searcher {
	dsn := os.Getenv("QZ_BENCH_DSN")
	if dsn == "" {
		b.Skip("QZ_BENCH_DSN 未设置")
	}
	db, err := store.OpenDB(dsn, 32, 8, 300)
	if err != nil {
		b.Skip(err)
	}
	mem := index.NewMemIndex(200000)
	mem.Build(db)

	cfg := config.SearchConfig{
		TopK: 20, BM25K1: 1.2, BM25B: 0.75, RRFK: 60,
		VectorWeight: 0.5, KeywordWeight: 1.0,
		SiteWeightW: 1.0, DynamicWeightW: 0.5, TrustWeightW: 1.0,
		FreshnessW: 0.3, SpamPenaltyW: 2.0, RerankTopN: 100, SnippetLen: 160,
	}
	se := New(db, nil, cfg, nil, "bench")
	se.SetMemIndex(mem)
	return se
}

var benchQueries = []string{
	"分布式搜索引擎", "倒排索引 分词", "golang 并发", "数据库 优化",
	"博客 周刊", "开源 项目", "人工智能 模型", "前端 框架",
}

func BenchmarkSearchLocal(b *testing.B) {
	s := benchSearcher(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := s.SearchLocal(ctx, benchQueries[i%len(benchQueries)], 20)
		if err != nil {
			b.Fatal(err)
		}
		_ = r
	}
}
