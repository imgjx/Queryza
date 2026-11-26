package algo

/*
#cgo CFLAGS: -I${SRCDIR}/../../algo
#cgo LDFLAGS: -L${SRCDIR}/../../algo -lqzalgo -lstdc++ -lm
#include <stdlib.h>
#include "qzalgo.h"
*/
import "C"

import (
	"math"
	"unsafe"
)

// Tokenize 调用 C++ 分词器，返回词项列表。
// 走紧凑格式（'\0' 分隔的连续缓冲），不经过 JSON，长文分词快数倍。
func Tokenize(text string) []string {
	if text == "" {
		return nil
	}
	cs := C.CString(text)
	defer C.free(unsafe.Pointer(cs))
	var cnt, size C.int
	p := C.qz_tokenize_flat(cs, &cnt, &size)
	if p == nil {
		return nil
	}
	defer C.qz_free(unsafe.Pointer(p))
	if cnt <= 0 || size <= 0 {
		return nil
	}
	buf := unsafe.Slice((*byte)(unsafe.Pointer(p)), int(size))
	out := make([]string, 0, int(cnt))
	start := 0
	for i := 0; i < len(buf); i++ {
		if buf[i] == 0 {
			if i > start {
				out = append(out, string(buf[start:i]))
			}
			start = i + 1
		}
	}
	if start < len(buf) {
		out = append(out, string(buf[start:]))
	}
	return out
}

// BM25 单文档得分。纯 Go 实现：搜索时每个候选行都要算一次，
// 走 cgo 的调用开销比算术本身还大。
func BM25(tf, df, docLen int, avgdl float64, N int, k1, b float64) float64 {
	if tf <= 0 || N <= 0 || docLen <= 0 {
		return 0
	}
	if df <= 0 {
		df = 1
	}
	if df > N {
		df = N
	}
	idf := math.Log(1.0 + (float64(N-df)+0.5)/(float64(df)+0.5))
	if avgdl <= 0 {
		avgdl = 1
	}
	denom := float64(tf) + k1*(1.0-b+b*(float64(docLen)/avgdl))
	if denom <= 0 {
		return 0
	}
	return idf * (float64(tf) * (k1 + 1.0)) / denom
}

// CosineBatch 计算 query 与多个文档向量的余弦相似度。
func CosineBatch(q []float32, docs [][]float32, dim int) []float64 {
	n := len(docs)
	if n == 0 || dim <= 0 || len(q) < dim {
		return nil
	}
	flat := make([]float32, n*dim)
	for i, d := range docs {
		copy(flat[i*dim:(i+1)*dim], d[:min(len(d), dim)])
	}
	out := C.qz_cosine_batch((*C.float)(unsafe.Pointer(&q[0])), (*C.float)(unsafe.Pointer(&flat[0])), C.int(n), C.int(dim))
	defer C.qz_free(unsafe.Pointer(out))
	cres := unsafe.Slice(out, n)
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = float64(cres[i])
	}
	return res
}

// RRF 融合分。
func RRF(rank int, k float64) float64 {
	if rank <= 0 {
		return 0
	}
	if k <= 0 {
		k = 60
	}
	return 1.0 / (k + float64(rank))
}

// Normalize 将分数集合做 min-max 归一化（用于混合排序融合）。
func Normalize(scores []float64) []float64 {
	if len(scores) == 0 {
		return scores
	}
	mn, mx := math.Inf(1), math.Inf(-1)
	for _, s := range scores {
		if s < mn {
			mn = s
		}
		if s > mx {
			mx = s
		}
	}
	out := make([]float64, len(scores))
	span := mx - mn
	for i, s := range scores {
		if span <= 1e-12 {
			out[i] = 0
		} else {
			out[i] = (s - mn) / span
		}
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
