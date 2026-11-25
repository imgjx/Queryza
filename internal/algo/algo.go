package algo

/*
#cgo CFLAGS: -I${SRCDIR}/../../algo
#cgo LDFLAGS: -L${SRCDIR}/../../algo -lqzalgo -lstdc++ -lm
#include <stdlib.h>
#include "qzalgo.h"
*/
import "C"

import (
	"encoding/json"
	"math"
	"unsafe"
)

// Tokenize 调用 C++ 分词器，返回词项列表。
func Tokenize(text string) []string {
	if text == "" {
		return nil
	}
	cs := C.CString(text)
	defer C.free(unsafe.Pointer(cs))
	p := C.qz_tokenize(cs)
	defer C.qz_free(unsafe.Pointer(p))
	js := C.GoString(p)
	var out []string
	if err := json.Unmarshal([]byte(js), &out); err != nil {
		return nil
	}
	return out
}

// BM25 单文档得分。
func BM25(tf, df, docLen int, avgdl float64, N int, k1, b float64) float64 {
	return float64(C.qz_bm25(C.int(tf), C.int(df), C.int(docLen), C.double(avgdl), C.int(N), C.double(k1), C.double(b)))
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
	return float64(C.qz_rrf(C.int(rank), C.double(k)))
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
