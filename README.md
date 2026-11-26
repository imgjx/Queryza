# Queryza

分布式全文搜索引擎，对外以 HTTP API 提供服务。主程序 Go，检索算法 C++，存储 MySQL + Redis，管理后台 Vue 3。

当前版本 **v0.1.1**，聚焦**文本搜索**（关键词 + 可选向量混合），单端口对外，支持单机与集群部署。

---

## 界面预览

| 系统概览 | 文档库 |
|---|---|
| ![](docs/screenshots/02-overview.jpg) | ![](docs/screenshots/03-docs.jpg) |

| 节点管理 | 清洗规则库 |
|---|---|
| ![](docs/screenshots/06-nodes.jpg) | ![](docs/screenshots/10-rules.jpg) |

| 域名分配（主副本 / 副本） | 搜索测试 |
|---|---|
| ![](docs/screenshots/11-assign.jpg) | ![](docs/screenshots/09-search.jpg) |

---

## 架构

```
                    ┌──────────────────────────────┐
   HTTP 单端口 ───▶ │  主节点 / standalone          │
   /api/v1/search   │  · API 网关 + 鉴权 + 配额      │
   {admin_path}     │  · 节点注册 / 心跳 / 密钥校验   │
                    │  · 域名多副本分配 + 再均衡      │
                    │  · fan-out 检索 + 归一化合并    │
                    └───────────┬──────────────────┘
                                │ /internal/search（HMAC 签名）
                 ┌──────────────┼──────────────┐
                 ▼              ▼              ▼
             ┌────────┐    ┌────────┐    ┌────────┐
             │ 节点 A │    │ 节点 B │    │ 节点 C │
             │ 抓取   │    │ 抓取   │    │ 抓取   │
             │ 索引   │    │ 索引   │    │ 索引   │
             │ 本地检索│   │ 本地检索│   │ 本地检索│
             └───┬────┘    └───┬────┘    └───┬────┘
                 └─────────────┼─────────────┘
                        MySQL（正排/倒排/元数据/副本归属）
                        Redis（热数据/查询缓存/QPS/配额）

域名 → 节点 是多对多：一个域名可在 N 个节点各存一份（replica_factor），
但检索只向「主副本」要结果，副本平时不参与召回，仅用于故障接管。
```

- **单端口**：管理后台、公开 API、内部接口共用 `server.addr` 一个端口；admin 路径启动时随机生成（可配置）。
- **域名多副本**：每个域名分配 `replica_factor` 个节点（默认 1），其中一个为**主副本**，负责抓取与入库；检索只向主副本要结果，避免同一文档被多节点重复召回。
- **故障接管**：主副本离线时，主节点先到持有该域名副本的节点代查（对调用方无感），心跳超时后自动把副本提为主副本。
- **再均衡**：新节点加入后，把主副本从最忙的节点挪一部分给最闲的节点，同时补齐副本数。
- **冷热分离**：冷数据落 MySQL；热数据/查询结果/热词落 Redis。
- **节点鉴权**：节点与主节点之间的 `/internal/*` 接口全部走 HMAC-SHA256 签名，密钥一节点一把，未注册或已禁用的节点直接 403。
- **中心调度单点**：主节点负责节点注册、心跳、域名分配与检索 fan-out；主节点不可用时集群无法调度（设计如此，后续可加主备）。

## 检索链路

```
query → 规范化 → C++ 分词 → 并行召回（倒排 BM25 / 向量 ANN）
      → 归一化融合 → 业务权重（站点/动态/信任/时效/作弊惩罚）→ TopK → 摘要
```

- 分词、BM25、余弦相似度、RRF 由 `algo/`（C++）提供，Go 通过 cgo 调用；算法层编为静态库并链入二进制。
- 单节点（standalone / 仅本机在线）直接返回本地原始分数；多节点时各节点结果 min-max 归一化后按节点权重合并去重。
- 向量检索：文档入库异步调用 OpenAI 兼容 `embeddings` 接口，向量存 MySQL BLOB；查询时对候选集做余弦计算（当前为暴力计算，规模上来后可替换为 HNSW）。

## 内容清洗规则库

正文抽取不是一套通用启发式，而是**规则库 + 通用兜底**：

1. 先按**域名后缀**匹配（`cnblogs.com`、`zhihu.com` …），最具体的优先；
2. 域名没命中时按**页面指纹**识别 CMS（`wp-content`、`Powered by Discuz`、`typecho` …）；
3. 都没命中才走**通用规则**：语义标签 → 常见正文类名 → 段落密度打分。

每条规则包含：正文选择器、需要剔除的区块选择器、id/class 关键词、标题选择器、最小长度。
规则命中的正文过短时自动回落通用规则，不会把页面洗成空。内置 **121 条**规则，覆盖
国内技术社区、新闻门户、论坛/CMS 引擎与常见海外站点；抓取日志里会记录命中的规则 ID，
后台「清洗规则」页可直接查看。

新增站点只需在 `internal/crawler/ruleset.go` 里加一条：

```go
{ID: "example", Name: "示例站", Priority: 100, Domains: []string{"example.com"},
    Content: []string{".article-body", "article"},
    Strip:   []string{".comment", ".sidebar"},
    Title:   []string{"h1"}, MinLen: 150},
```

## 目录

```
cmd/queryza/          程序入口（角色、参数、依赖装配）
internal/config/      配置加载 + 节点级覆盖 + 运行时热更新
internal/store/       MySQL schema / 连接、Redis 封装
internal/algo/        cgo 封装 C++ 算法
internal/crawler/     抓取器（请求头/代理组/robots）、HTML 解析、Frontier、主循环
internal/index/       索引写入、Embedding 客户端
internal/search/      本地检索与排序
internal/cluster/     中心调度、节点 Agent、fan-out 合并
internal/api/         单端口路由、管理 API、内部 API
internal/auth/        API Key 与管理员登录态
algo/                 C++ 算法源码
frontend/admin/       Vue 3 管理后台（内嵌）
seeds/seeds.json      基本索引表（种子站点）
configs/config.yaml   系统配置
```

## 依赖

- Go 1.21+
- g++（C++17）
- MySQL / MariaDB
- Redis

## 安装（预编译包）

从 [Releases](https://github.com/imgjx/Queryza/releases) 下载 `queryza-<version>-linux-amd64.tar.gz`：

```bash
tar -xzf queryza-0.1.0-linux-amd64.tar.gz
cd queryza-0.1.0-linux-amd64

# 准备依赖：MySQL/MariaDB + Redis
mysql -uroot -e "CREATE DATABASE queryza CHARACTER SET utf8mb4;
  CREATE USER 'queryza'@'localhost' IDENTIFIED BY 'queryza';
  GRANT ALL ON queryza.* TO 'queryza'@'localhost';"

./start.sh                      # 读取 configs/config.yaml 启动
# 或指定端口与后台路径
./queryza -config configs/config.yaml -addr :8080 -admin-path /qz-admin
```

预编译二进制已静态链接 C++ 算法层，不依赖 `libqzalgo.so`，只需系统具备
glibc / libstdc++（主流发行版默认自带）。

## 从源码构建

```bash
# 1. 编译 C++ 算法层（静态库）
make algo          # 生成 algo/libqzalgo.a

# 2. 初始化数据库
mysql -uroot -e "CREATE DATABASE queryza CHARACTER SET utf8mb4;
  CREATE USER 'queryza'@'localhost' IDENTIFIED BY 'queryza';
  GRANT ALL ON queryza.* TO 'queryza'@'localhost';"

# 3. 编译主程序
make build         # 生成 bin/queryza，版本号由 VERSION 注入

# 4. 运行
make run

# 5. 打包发布产物（可选）
make dist          # 生成 dist/queryza-<version>-linux-amd64.tar.gz
```

启动后终端会打印随机管理后台路径与登录账号（默认 `admin / admin123`，可用环境变量
`QZ_ADMIN_USER` / `QZ_ADMIN_PASS` 覆盖）。

## 使用

### 1. 管理后台

浏览器打开终端打印的 `http://127.0.0.1:8080/qz-xxxxxxxx`，登录后可：

- 查看索引规模、抓取进度、节点状态
- 启动/停止抓取、手动入队 URL
- 管理种子站点
- 搜索测试（观察分词、BM25、向量分、来源）
- 创建 API 密钥（配额/QPS/结果上限）
- 修改检索权重与抓取配置

### 2. 调用检索 API

先在后台创建密钥，然后：

```bash
curl -H "X-API-Key: qz_xxx" \
  "http://127.0.0.1:8080/api/v1/search?q=搜索引擎&top_k=10"
```

响应：

```json
{
  "query": "搜索引擎",
  "tokens": ["搜索", "索引", "引擎"],
  "mode": "keyword",
  "total": 12,
  "took_ms": 3,
  "hits": [
    {"doc_id":1,"url":"...","title":"...","domain":"...","snippet":"...",
     "score":2.1,"bm25":0.8,"vector":0,"site_weight":1.3,"source":"keyword"}
  ]
}
```

### 3. 集群部署

主节点：

```bash
./bin/queryza -config configs/config.yaml -role master -node-id master \
  -addr :8080 -advertise http://10.0.0.1:8080 -admin-path /my-admin -replica 2
```

子节点不再手写密钥，改由后台发放**一次性加入令牌**：

1. 后台「集群 / 节点」→ 添加节点，填节点 ID 与对外地址；
2. 复制生成的命令到目标机器执行：

```bash
./queryza -config configs/config.yaml -role node -node-id node-2 \
  -addr :8081 -advertise http://10.0.0.2:8081 \
  -master "https://cdn-a.example.com,https://cdn-b.example.com,http://10.0.0.1:8080" \
  -token <一次性令牌>
```

节点首次启动用令牌换取密钥，写入 `data/node_identity.json`，之后启动不再需要 token。

> 节点**不带管理界面**：它只监听 `/internal/*` 内部接口，其余路径一律 404。
> 抓取开关由主节点后台统一下发到各节点，各节点只领取自己负责的域名。

**主地址表**按顺序尝试：前面的是 CDN 入口，最后可以放直连 IP；某个入口不通就自动切下一个，
每分钟回到表首重试（CDN 套餐恢复后会自动切回去）。

**多副本**：`-replica 2` 表示每个域名在两个节点各存一份。抓取由主副本负责，
检索只问主副本；主副本挂掉时先由副本代查，心跳超时后副本自动提为主副本。

**节点鉴权**：`/internal/register|heartbeat|search` 全部要求签名头
（`X-QZ-Node` / `X-QZ-Ts` / `X-QZ-Sign`），密钥一节点一把，时间戳偏移超过 5 分钟直接拒绝。

## 配置要点

| 配置 | 说明 |
|---|---|
| `server.admin_path_random` | 为 true 时启动随机生成 admin 路径 |
| `crawler.mode` | `targeted` 只抓种子站点、关闭域名自发现；`full` 全网扩散 |
| `crawler.default_headers` | 全局默认请求头；`nodes.<id>.headers` 可按节点覆盖 |
| `crawler.proxy_group` | 代理组名；代理列表存 `system_config.proxy_groups`（JSON） |
| `crawler.block_domains` | 黑名单，两种模式均生效 |
| `search.*` | BM25 参数、各路权重、时效与作弊惩罚 |
| `embedding.*` | OpenAI 兼容向量接口（base_url / api_key / model / dim） |
| `limits.max_db_docs_per_node` | 单节点最大文档数 |
| `limits.max_redis_docs` | Redis 热数据上限 |
| `cluster.replica_factor` | 每个域名的副本数，1 为单副本 |
| `cluster.master_addrs` | 主节点地址表，按顺序故障切换 |
| `cluster.require_auth` | 是否强制内部接口签名，默认 true |

### 代理组配置

代理组存在数据库 `system_config` 表（key=`proxy_groups`），格式：

```json
{
  "default": ["http://user:pass@1.1.1.1:8080", "socks5://2.2.2.2:1080"],
  "cn": ["http://3.3.3.3:3128"]
}
```

然后 `crawler.proxy_group: "default"` 或节点级覆盖。抓取时按轮询选取代理。

### 节点级配置

`configs/config.yaml` 的 `nodes` 段：

```yaml
nodes:
  node-1:
    weight: 2
    crawler_enabled: true
    proxy_group: "cn"
    headers:
      User-Agent: "CustomBot/1.0"
    max_db_docs: 500000
```

未配置的字段继承系统总配置。

## 已知限制

- 向量检索当前为候选集暴力余弦；文档量到百万级需换 HNSW / 专用向量库。
- 中心调度为单点，无自动主备切换。
- 倒排索引存 MySQL，大规模下建议改为专用索引文件或列存。
- 部分站点（如 52pojie）有 Cloudflare 防护，默认请求头可能返回 403，需配置 Cookie 或代理。
- 中文分词为 bigram 方案，无词典与词性，适合通用召回，专业领域可替换为词典分词。

## 实测

本机双节点（MariaDB + Redis，master + node-1，`replica_factor=2`）：

| 项目 | 结果 |
|---|---|
| 抓取 | 4000+ 次请求，2600+ 页面成功；17 个站点命中各自的专属规则 |
| 入库 | 2200+ 篇，两个节点分片各占一半（约 1100 / 1100） |
| 检索 | 集群 fan-out 合并 7ms，跨节点结果无重复 |
| 副本 | 每个域名 1 主 1 副；主副本宕机后由副本代查，结果不中断 |
| 鉴权 | 无签名 / 伪造签名 / 未注册节点均返回 401 |
| 规则命中 | cnblogs、ithome、infoq、jianshu、douban、sspai 等按各自规则抽取 |

单节点回归同样通过：`mode=keyword`、原始 BM25 分、P99 < 10ms。
