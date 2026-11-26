#ifndef QZALGO_H
#define QZALGO_H

#ifdef __cplusplus
extern "C" {
#endif

/* 分词：返回 malloc 分配的 JSON 数组字符串，调用方用 qz_free 释放。
 * 中文按 bigram 切分（单字段保留单字），英文/数字按词切分并转小写，过滤停用词。 */
char* qz_tokenize(const char* text);

/* 分词（紧凑格式，推荐）：返回 malloc 分配的连续缓冲区，词项以 '\0' 分隔，
 * *out_count 返回词项个数，*out_size 返回缓冲区字节数（不含末尾 '\0'）。
 * 相比 JSON 输出省掉转义与序列化/反序列化，吞吐高数倍。 */
char* qz_tokenize_flat(const char* text, int* out_count, int* out_size);

/* BM25 单文档得分 */
double qz_bm25(int tf, int df, int doclen, double avgdl, int N, double k1, double b);

/* 批量余弦相似度：q 为 dim 维，docs 为 n*dim 连续数组，返回 malloc 的 double[n] */
double* qz_cosine_batch(const float* q, const float* docs, int n, int dim);

/* Reciprocal Rank Fusion：给定 ranks（1-based 名次），返回融合分 */
double qz_rrf(int rank, double k);

/* 释放本库分配的内存 */
void qz_free(void* p);

const char* qz_version(void);

#ifdef __cplusplus
}
#endif
#endif
