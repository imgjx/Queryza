package crawler

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 基准测试用的真实页面样本目录，未设置则跳过。
func benchSamples(b *testing.B) [][]byte {
	dir := os.Getenv("QZ_BENCH_PAGES")
	if dir == "" {
		b.Skip("QZ_BENCH_PAGES 未设置")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		b.Skip(err)
	}
	var out [][]byte
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".html") {
			continue
		}
		p := filepath.Join(dir, e.Name())
		if fi, err := os.Stat(p); err == nil && fi.Size() > 20000 {
			if bt, err := os.ReadFile(p); err == nil {
				out = append(out, bt)
			}
		}
	}
	if len(out) == 0 {
		b.Skip("无样本")
	}
	return out
}

func BenchmarkParse(b *testing.B) {
	pages := benchSamples(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := Parse("https://example.com/post/1", pages[i%len(pages)])
		if p == nil {
			b.Fatal("nil page")
		}
	}
}

func BenchmarkLowerHTML(b *testing.B) {
	pages := benchSamples(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lowerHTML(pages[i%len(pages)])
	}
}
