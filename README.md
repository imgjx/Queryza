# Queryza

分布式全文搜索引擎，对外以 HTTP API 提供服务。主程序 Go，检索算法 C++，存储 MySQL + Redis，管理后台 Vue 3。

当前版本 **v0.1.0**，聚焦**文本搜索**（关键词 + 可选向量混合），单端口对外，支持单机与集群部署。

---

## 界面预览

| 系统概览 | 文档库 |
|---|---|
| ![](docs/screenshots/02-overview.jpg) | ![](docs/screenshots/03-docs.jpg) |

| 节点管理 | 搜索测试 |
|---|---|
| ![](docs/screenshots/06-nodes.jpg) | ![](docs/screenshots/09-search.jpg) |

---

## 架构

```
                    ┌──────────────────────────────┐
   HTTP 单端口 ───▶ │  主节点 / standalone          │
   /api/v1/search   │  · API 网关 + 鉴权 + 配额      │
   {admin_path}     │  · 中心调度：节点注册 / 心跳    │
                    │  · 域名分配                    │
                    │  · fan-out 检索 + 归一化合并    │
                    └───────────┬──────────────────┘
                                │ /internal/search
                 ┌──────────────┼──────────────┐
                 ▼              ▼              ▼
             ┌────────┐    ┌────────┐    ┌────────┐
             │ 节点 A │    │ 节点 B │    │ 节点 C │
             │ 抓取   │    │ 抓取   │    │ 抓取   │
             │ 索引   │    │ 索引   │    │ 索引   │
             │ 本地检索│   │ 本地检索│   │ 本地检索│
             └───┬────┘    └───┬────┘    └───┬────┘
                 └─────────────┼─────────────┘
                        MySQL（正排/倒排/元数据）
                        Redis（热数据/查询缓存/QPS/配额）
```

- **单端口**：管理后台、公开 API、内部接口共用 `server.addr` 一个端口；admin 路径启动时随机生成（可配置）。
- **域名分片**：每个域名默认归属一个节点，可配置为多节点。
- **冷热分离**：冷数据落 MySQL；热数据/查询结果/热词落 Redis。
- **中心调度单点**：主节点负责节点注册、心跳、域名分配与检索 fan-out；主节点不可用时集群无法调度（设计如此，后续可加主备）。

## 检索链路

```
query → 规范化 → C++ 分词 → 并行召回（倒排 BM25 / 向量 ANN）
      → 归一化融合 → 业务权重（站点/动态/信任/时效/作弊惩罚）→ TopK → 摘要
```

- 分词、BM25、余弦相似度、RRF 由 `algo/`（C++）提供，Go 通过 cgo 调用；算法层编为静态库并链入二进制。
- 单节点（standalone / 仅本机在线）直接返回本地原始分数；多节点时各节点结果 min-max 归一化后按节点权重合并去重。
- 向量检索：文档入库异步调用 OpenAI 兼容 `embeddings` 接口，向量存 MySQL BLOB；查询时对候选集做余弦计算（当前为暴力计算，规模上来后可替换为 HNSW）。

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
./bin/queryza -config configs/config.yaml -role master -node-id master-1 \
  -addr :8080 -advertise http://10.0.0.1:8080 -admin-path /my-admin
```

子节点：

```bash
./bin/queryza -config configs/config.yaml -role node -node-id node-1 \
  -addr :8081 -advertise http://10.0.0.2:8081 -master http://10.0.0.1:8080
```

子节点启动后自动注册并每 10 秒心跳；主节点按「域名数最少」分配新域名。
检索时主节点向所有在线节点 fan-out，各节点返回归一化结果，主节点去重合并。

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

在本机（MariaDB + Redis）实抓种子站点扩散抓取：入库 111 篇中文页面、覆盖 12 个域名，
关键词检索 P99 < 10ms（本地库），管理后台、API 鉴权、抓取队列、集群注册与 fan-out 合并均已跑通。
