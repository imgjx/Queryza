// Queryza 搜索算法核心：UTF-8 分词、BM25、余弦相似度、RRF 融合
// 编译：g++ -O3 -std=c++17 -fPIC -shared -o libqzalgo.so qzalgo.cpp
#include "qzalgo.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <string_view>
#include <vector>
#include <unordered_set>

namespace {

struct Rune {
    uint32_t cp;
    int len;
};

// 解码一个 UTF-8 码点；非法字节按单字节处理
Rune decode(const unsigned char* s, int remain) {
    Rune r{0, 1};
    unsigned char c = s[0];
    if (c < 0x80) { r.cp = c; r.len = 1; return r; }
    int n = 0;
    if ((c & 0xE0) == 0xC0) { r.cp = c & 0x1F; n = 1; }
    else if ((c & 0xF0) == 0xE0) { r.cp = c & 0x0F; n = 2; }
    else if ((c & 0xF8) == 0xF0) { r.cp = c & 0x07; n = 3; }
    else { r.cp = c; r.len = 1; return r; }
    if (remain < n + 1) { r.cp = c; r.len = 1; return r; }
    for (int i = 1; i <= n; ++i) {
        if ((s[i] & 0xC0) != 0x80) { r.cp = c; r.len = 1; return r; }
        r.cp = (r.cp << 6) | (s[i] & 0x3F);
    }
    r.len = n + 1;
    return r;
}

bool is_ascii_alnum(unsigned char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
}

bool is_cjk(uint32_t cp) {
    return (cp >= 0x4E00 && cp <= 0x9FFF) ||   // CJK 统一表意
           (cp >= 0x3400 && cp <= 0x4DBF) ||   // 扩展 A
           (cp >= 0xF900 && cp <= 0xFAFF) ||   // 兼容表意
           (cp >= 0x3040 && cp <= 0x30FF) ||   // 日文假名
           (cp >= 0xAC00 && cp <= 0xD7AF);     // 韩文
}

// 中文单字停用
const std::unordered_set<std::string_view>& cn_stop() {
    static const std::unordered_set<std::string_view> s = {
        "的","了","和","是","在","有","就","不","人","都","一","上","也","很","到",
        "说","要","去","你","会","着","看","好","这","那","之","与","及","等","被",
        "把","让","对","从","而","或","但","因","为","我","他","她","它","们","个",
        "中","下","大","小","多","少","吗","呢","啊","吧","哦","呀","么","得","地",
        "过","还","又","才","再","可","以","于","其","此","所","者","也","已","将",
        "能","该","应","若","即","并","且","由","至","向","于","自","同","给","跟"
    };
    return s;
}

// 英文停用词
const std::unordered_set<std::string_view>& en_stop() {
    static const std::unordered_set<std::string_view> s = {
        "the","a","an","and","or","of","to","in","is","are","was","were","be","been",
        "being","for","on","at","by","with","as","from","that","this","these","those",
        "it","its","i","you","he","she","they","we","my","your","his","her","their",
        "our","not","no","but","if","then","so","such","than","too","very","can","will",
        "just","do","does","did","has","have","had","what","which","who","whom","how","why"
    };
    return s;
}

void json_escape_append(std::string& out, const char* s, int len) {
    for (int i = 0; i < len; ++i) {
        unsigned char c = (unsigned char)s[i];
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += (char)c;
                }
        }
    }
}

void emit_token(std::string& out, bool& first, const std::string& tok) {
    if (tok.empty()) return;
    if (!first) out += ',';
    first = false;
    out += '"';
    json_escape_append(out, tok.data(), (int)tok.size());
    out += '"';
}

} // namespace

extern "C" {

char* qz_tokenize(const char* text) {
    std::string out = "[";
    bool first = true;
    if (!text) { out += "]"; char* r = (char*)malloc(3); memcpy(r, "[]", 3); return r; }

    const unsigned char* p = (const unsigned char*)text;
    int n = (int)strlen(text);
    int i = 0;

    std::string ascii;
    std::vector<std::string> cjk_run; // 连续中文字，每项一个字

    auto flush_ascii = [&]() {
        if (!ascii.empty()) {
            std::string low = ascii;
            for (auto& ch : low) ch = (char)tolower((unsigned char)ch);
            if (low.size() > 1 && !en_stop().count(low)) emit_token(out, first, low);
            else if (low.size() == 1 && !en_stop().count(low) && !isdigit((unsigned char)low[0]))
                emit_token(out, first, low);
            ascii.clear();
        }
    };
    auto flush_cjk = [&]() {
        if (cjk_run.empty()) return;
        if (cjk_run.size() == 1) {
            const std::string& c = cjk_run[0];
            if (!cn_stop().count(c)) emit_token(out, first, c);
        } else {
            for (size_t k = 0; k + 1 < cjk_run.size(); ++k) {
                const std::string& a = cjk_run[k];
                const std::string& b = cjk_run[k + 1];
                if (cn_stop().count(a) && cn_stop().count(b)) continue;
                emit_token(out, first, a + b);
            }
        }
        cjk_run.clear();
    };

    while (i < n) {
        unsigned char c = p[i];
        if (c < 0x80) {
            if (is_ascii_alnum(c)) {
                flush_cjk();
                ascii += (char)c;
                i += 1;
            } else {
                flush_ascii();
                flush_cjk();
                i += 1;
            }
        } else {
            Rune r = decode(p + i, n - i);
            if (is_cjk(r.cp)) {
                flush_ascii();
                cjk_run.push_back(std::string((const char*)p + i, r.len));
            } else {
                flush_ascii();
                flush_cjk();
            }
            i += r.len;
        }
    }
    flush_ascii();
    flush_cjk();

    out += "]";
    char* res = (char*)malloc(out.size() + 1);
    memcpy(res, out.data(), out.size());
    res[out.size()] = '\0';
    return res;
}

// ---------- 紧凑分词：'\0' 分隔，零 JSON 开销 ----------

namespace {

struct Slice {
    const char* p;
    int len;
};

// 停用词用 string_view 查表，避免为每个单字构造 std::string
inline void emit_sv(std::string& out, std::string_view sv) {
    out.append(sv.data(), sv.size());
    out.push_back('\0');
}

} // namespace

char* qz_tokenize_flat(const char* text, int* out_count, int* out_size) {
    if (out_count) *out_count = 0;
    if (out_size) *out_size = 0;
    std::string out;
    if (!text) {
        char* r = (char*)malloc(1);
        r[0] = '\0';
        return r;
    }

    const unsigned char* p = (const unsigned char*)text;
    int n = (int)strlen(text);
    int i = 0;
    int count = 0;

    std::string ascii;
    std::vector<Slice> cjk_run;
    cjk_run.reserve(64);

    auto flush_ascii = [&]() {
        if (ascii.empty()) return;
        for (auto& ch : ascii) ch = (char)tolower((unsigned char)ch);
        std::string_view sv(ascii);
        if (sv.size() > 1) {
            if (!en_stop().count(sv)) { emit_sv(out, sv); count++; }
        } else if (!en_stop().count(sv) && !isdigit((unsigned char)sv[0])) {
            emit_sv(out, sv);
            count++;
        }
        ascii.clear();
    };

    auto flush_cjk = [&]() {
        if (cjk_run.empty()) return;
        const auto& stop = cn_stop();
        if (cjk_run.size() == 1) {
            std::string_view a(cjk_run[0].p, cjk_run[0].len);
            if (!stop.count(a)) { emit_sv(out, a); count++; }
        } else {
            char buf[8];
            for (size_t k = 0; k + 1 < cjk_run.size(); ++k) {
                const Slice& a = cjk_run[k];
                const Slice& b = cjk_run[k + 1];
                std::string_view av(a.p, a.len), bv(b.p, b.len);
                if (stop.count(av) && stop.count(bv)) continue;
                int len = a.len + b.len;
                memcpy(buf, a.p, a.len);
                memcpy(buf + a.len, b.p, b.len);
                out.append(buf, len);
                out.push_back('\0');
                count++;
            }
        }
        cjk_run.clear();
    };

    while (i < n) {
        unsigned char c = p[i];
        if (c < 0x80) {
            if (is_ascii_alnum(c)) {
                flush_cjk();
                ascii += (char)c;
                i += 1;
            } else {
                flush_ascii();
                flush_cjk();
                i += 1;
            }
        } else {
            Rune r = decode(p + i, n - i);
            if (is_cjk(r.cp)) {
                flush_ascii();
                cjk_run.push_back(Slice{(const char*)p + i, r.len});
            } else {
                flush_ascii();
                flush_cjk();
            }
            i += r.len;
        }
    }
    flush_ascii();
    flush_cjk();

    size_t sz = out.size();
    char* res = (char*)malloc(sz + 1);
    if (!res) return nullptr;
    if (sz) memcpy(res, out.data(), sz);
    res[sz] = '\0';
    if (out_count) *out_count = count;
    if (out_size) *out_size = (int)sz;
    return res;
}

double qz_bm25(int tf, int df, int doclen, double avgdl, int N, double k1, double b) {
    if (tf <= 0 || N <= 0 || doclen <= 0) return 0.0;
    if (df <= 0) df = 1;
    if (df > N) df = N;
    double idf = std::log(1.0 + (N - df + 0.5) / (df + 0.5));
    double denom = tf + k1 * (1.0 - b + b * (doclen / (avgdl > 0 ? avgdl : 1.0)));
    return idf * (tf * (k1 + 1.0)) / (denom > 0 ? denom : 1.0);
}

double* qz_cosine_batch(const float* q, const float* docs, int n, int dim) {
    double* out = (double*)malloc(sizeof(double) * (n > 0 ? n : 1));
    if (!out) return nullptr;
    double qn = 0.0;
    for (int d = 0; d < dim; ++d) qn += (double)q[d] * q[d];
    qn = std::sqrt(qn);
    for (int i = 0; i < n; ++i) {
        const float* v = docs + (size_t)i * dim;
        double dot = 0.0, vn = 0.0;
        for (int d = 0; d < dim; ++d) {
            dot += (double)q[d] * v[d];
            vn += (double)v[d] * v[d];
        }
        vn = std::sqrt(vn);
        out[i] = (qn > 0 && vn > 0) ? dot / (qn * vn) : 0.0;
    }
    return out;
}

double qz_rrf(int rank, double k) {
    if (rank <= 0) return 0.0;
    if (k <= 0) k = 60.0;
    return 1.0 / (k + (double)rank);
}

void qz_free(void* p) { free(p); }

const char* qz_version(void) { return "queryza-algo-1.0"; }

} // extern "C"
