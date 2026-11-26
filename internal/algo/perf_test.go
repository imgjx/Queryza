package algo

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func benchText(b *testing.B) string {
	dir := os.Getenv("QZ_BENCH_PAGES")
	if dir == "" {
		b.Skip("QZ_BENCH_PAGES 未设置")
	}
	entries, _ := os.ReadDir(dir)
	var all []byte
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".html") {
			continue
		}
		if bt, err := os.ReadFile(filepath.Join(dir, e.Name())); err == nil {
			all = append(all, bt...)
		}
	}
	if len(all) == 0 {
		b.Skip("无样本")
	}
	return string(all)
}

func BenchmarkTokenize(b *testing.B) {
	text := benchText(b)
	b.SetBytes(int64(len(text)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := Tokenize(text); len(got) == 0 {
			b.Fatal("空分词结果")
		}
	}
}
