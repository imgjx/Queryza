package index

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"queryza/internal/algo"
	"queryza/internal/config"
	"queryza/internal/store"
)

func benchDB(b *testing.B) *store.DB {
	dsn := os.Getenv("QZ_BENCH_DSN")
	if dsn == "" {
		b.Skip("QZ_BENCH_DSN 未设置")
	}
	db, err := store.OpenDB(dsn, 32, 8, 300)
	if err != nil {
		b.Skip(err)
	}
	return db
}

// 生成一篇约 1200 个词项的模拟文档
func fakeDoc(i int) (*store.Document, []string) {
	var sb strings.Builder
	for j := 0; j < 120; j++ {
		fmt.Fprintf(&sb, "分布式搜索引擎第%d篇文档的第%d段内容，涉及倒排索引、分词、相关性排序与集群调度。", i, j)
	}
	text := sb.String()
	tokens := algo.Tokenize(text)
	return &store.Document{
		URL: fmt.Sprintf("https://bench.invalid/doc/%d/%d", i, time.Now().UnixNano()),
		Domain: "bench.invalid", Title: fmt.Sprintf("基准文档 %d", i),
		Content: text, ContentLen: len(tokens), FetchedAt: time.Now(),
		SiteWeight: 1, DynamicWeight: 1,
	}, tokens
}

func BenchmarkIndexDocument(b *testing.B) {
	db := benchDB(b)
	ix := NewIndexer(db, nil, config.LimitsConfig{MaxRedisDocs: 20000}, nil)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc, tokens := fakeDoc(i)
		if err := ix.IndexDocument(ctx, doc, tokens); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	db.Exec(`DELETE p FROM postings p JOIN documents d ON d.id=p.doc_id WHERE d.domain='bench.invalid'`)
	db.Exec(`DELETE FROM documents WHERE domain='bench.invalid'`)
}

func BenchmarkIndexDocumentParallel(b *testing.B) {
	db := benchDB(b)
	ix := NewIndexer(db, nil, config.LimitsConfig{MaxRedisDocs: 20000}, nil)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			doc, tokens := fakeDoc(i)
			if err := ix.IndexDocument(context.Background(), doc, tokens); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	db.Exec(`DELETE p FROM postings p JOIN documents d ON d.id=p.doc_id WHERE d.domain='bench.invalid'`)
	db.Exec(`DELETE FROM documents WHERE domain='bench.invalid'`)
}
