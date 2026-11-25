package config

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 为系统总配置；节点未覆盖的字段一律继承此处。
type Config struct {
	Server    ServerConfig              `yaml:"server"`
	Database  DatabaseConfig            `yaml:"database"`
	Redis     RedisConfig               `yaml:"redis"`
	Cluster   ClusterConfig             `yaml:"cluster"`
	Crawler   CrawlerConfig             `yaml:"crawler"`
	Search    SearchConfig              `yaml:"search"`
	Embedding EmbeddingConfig           `yaml:"embedding"`
	Limits    LimitsConfig              `yaml:"limits"`
	Nodes     map[string]NodeOverride   `yaml:"nodes"`
	raw       map[string]interface{}
}

type ServerConfig struct {
	Addr            string `yaml:"addr"`
	AdminPath       string `yaml:"admin_path"`
	AdminPathRandom bool   `yaml:"admin_path_random"`
	ReadTimeoutSec  int    `yaml:"read_timeout_sec"`
	WriteTimeoutSec int    `yaml:"write_timeout_sec"`
}

type DatabaseConfig struct {
	DSN             string `yaml:"dsn"`
	MaxOpenConns    int    `yaml:"max_open_conns"`
	MaxIdleConns    int    `yaml:"max_idle_conns"`
	ConnMaxLifetime int    `yaml:"conn_max_lifetime_sec"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	PoolSize int    `yaml:"pool_size"`
}

type ClusterConfig struct {
	Role          string  `yaml:"role"` // standalone | master | node
	NodeID        string  `yaml:"node_id"`
	NodeName      string  `yaml:"node_name"`
	MasterAddr    string  `yaml:"master_addr"`
	AdvertiseAddr string  `yaml:"advertise_addr"`
	Weight        float64 `yaml:"weight"`
	HeartbeatSec  int     `yaml:"heartbeat_sec"`
	FanoutMode    string  `yaml:"fanout_mode"` // all | weighted
}

type CrawlerConfig struct {
	Enabled        bool              `yaml:"enabled"`
	Mode           string            `yaml:"mode"` // full | targeted
	UserAgent      string            `yaml:"user_agent"`
	TimeoutSec     int               `yaml:"timeout_sec"`
	MaxDepth       int               `yaml:"max_depth"`
	MaxPagesPerRun int               `yaml:"max_pages_per_run"`
	Concurrency    int               `yaml:"concurrency"`
	RespectRobots  bool              `yaml:"respect_robots"`
	DefaultHeaders map[string]string `yaml:"default_headers"`
	ProxyGroup     string            `yaml:"proxy_group"`
	AllowDomains   []string          `yaml:"allow_domains"`
	BlockDomains   []string          `yaml:"block_domains"`
	SeedFile       string            `yaml:"seed_file"`
	AutoStart      bool              `yaml:"auto_start"`
}

type SearchConfig struct {
	TopK           int     `yaml:"top_k"`
	BM25K1         float64 `yaml:"bm25_k1"`
	BM25B          float64 `yaml:"bm25_b"`
	RRFK           float64 `yaml:"rrf_k"`
	VectorWeight   float64 `yaml:"vector_weight"`
	KeywordWeight  float64 `yaml:"keyword_weight"`
	SiteWeightW    float64 `yaml:"site_weight_w"`
	DynamicWeightW float64 `yaml:"dynamic_weight_w"`
	TrustWeightW   float64 `yaml:"trust_weight_w"`
	FreshnessW     float64 `yaml:"freshness_w"`
	SpamPenaltyW   float64 `yaml:"spam_penalty_w"`
	Hybrid         bool    `yaml:"hybrid"`
	RerankTopN     int     `yaml:"rerank_top_n"`
	SnippetLen     int     `yaml:"snippet_len"`
}

type EmbeddingConfig struct {
	Enabled bool   `yaml:"enabled"`
	BaseURL string `yaml:"base_url"` // OpenAI 兼容 /v1/embeddings
	APIKey  string `yaml:"api_key"`
	Model   string `yaml:"model"`
	Dim     int    `yaml:"dim"`
	Timeout int    `yaml:"timeout_sec"`
}

type LimitsConfig struct {
	MaxDBDocsPerNode  int `yaml:"max_db_docs_per_node"`
	MaxRedisDocs      int `yaml:"max_redis_docs"`
	MaxRedisHot       int `yaml:"max_redis_hot"`
	CrawlQueueMax     int `yaml:"crawl_queue_max"`
}

// NodeOverride 节点级覆盖：只写需要覆盖的字段。
type NodeOverride struct {
	Weight         *float64          `yaml:"weight"`
	CrawlerEnabled *bool             `yaml:"crawler_enabled"`
	ProxyGroup     *string           `yaml:"proxy_group"`
	Headers        map[string]string `yaml:"headers"`
	MaxDBDocs      *int              `yaml:"max_db_docs"`
	MaxRedisDocs   *int              `yaml:"max_redis_docs"`
	Mode           *string           `yaml:"mode"`
}

func defaults() *Config {
	return &Config{
		Server: ServerConfig{Addr: ":8080", AdminPath: "", AdminPathRandom: true, ReadTimeoutSec: 15, WriteTimeoutSec: 60},
		Database: DatabaseConfig{
			DSN: "queryza:queryza@tcp(127.0.0.1:3306)/queryza?charset=utf8mb4&parseTime=true&loc=Local",
			MaxOpenConns: 64, MaxIdleConns: 16, ConnMaxLifetime: 300,
		},
		Redis: RedisConfig{Addr: "127.0.0.1:6379", DB: 0, PoolSize: 64},
		Cluster: ClusterConfig{Role: "standalone", NodeID: "node-local", NodeName: "本地节点", Weight: 1, HeartbeatSec: 10, FanoutMode: "all"},
		Crawler: CrawlerConfig{
			Enabled: true, Mode: "targeted", TimeoutSec: 15, MaxDepth: 2, MaxPagesPerRun: 200,
			Concurrency: 8, RespectRobots: true, SeedFile: "seeds/seeds.json",
			DefaultHeaders: map[string]string{
				"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
				"Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
				"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
			},
		},
		Search: SearchConfig{
			TopK: 20, BM25K1: 1.2, BM25B: 0.75, RRFK: 60, VectorWeight: 0.5, KeywordWeight: 1.0,
			SiteWeightW: 1.0, DynamicWeightW: 0.5, TrustWeightW: 1.0, FreshnessW: 0.3, SpamPenaltyW: 2.0,
			Hybrid: true, RerankTopN: 100, SnippetLen: 160,
		},
		Embedding: EmbeddingConfig{Enabled: false, BaseURL: "https://api.openai.com/v1", Model: "text-embedding-3-small", Dim: 1536, Timeout: 20},
		Limits:    LimitsConfig{MaxDBDocsPerNode: 1000000, MaxRedisDocs: 20000, MaxRedisHot: 200, CrawlQueueMax: 500000},
		Nodes:     map[string]NodeOverride{},
	}
}

func Load(path string) (*Config, error) {
	c := defaults()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("读取配置失败: %w", err)
		}
		if err := yaml.Unmarshal(b, c); err != nil {
			return nil, fmt.Errorf("解析配置失败: %w", err)
		}
	}
	c.normalize()
	return c, nil
}

func (c *Config) normalize() {
	if c.Server.AdminPath == "" {
		if c.Server.AdminPathRandom {
			c.Server.AdminPath = "/qz-" + randToken(12)
		} else {
			c.Server.AdminPath = "/admin"
		}
	}
	if c.Cluster.Weight <= 0 {
		c.Cluster.Weight = 1
	}
	if c.Cluster.FanoutMode == "" {
		c.Cluster.FanoutMode = "all"
	}
}

func randToken(n int) string {
	const alpha = "abcdefghijklmnopqrstuvwxyz0123456789"
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, n)
	for i := range b {
		b[i] = alpha[r.Intn(len(alpha))]
	}
	return string(b)
}

// ApplyNodeOverride 将节点级配置覆盖到主配置副本上。
func (c *Config) ApplyNodeOverride(nodeID string) {
	o, ok := c.Nodes[nodeID]
	if !ok {
		return
	}
	if o.Weight != nil {
		c.Cluster.Weight = *o.Weight
	}
	if o.CrawlerEnabled != nil {
		c.Crawler.Enabled = *o.CrawlerEnabled
	}
	if o.ProxyGroup != nil {
		c.Crawler.ProxyGroup = *o.ProxyGroup
	}
	if o.Mode != nil {
		c.Crawler.Mode = *o.Mode
	}
	if o.MaxDBDocs != nil {
		c.Limits.MaxDBDocsPerNode = *o.MaxDBDocs
	}
	if o.MaxRedisDocs != nil {
		c.Limits.MaxRedisDocs = *o.MaxRedisDocs
	}
	if len(o.Headers) > 0 {
		if c.Crawler.DefaultHeaders == nil {
			c.Crawler.DefaultHeaders = map[string]string{}
		}
		for k, v := range o.Headers {
			c.Crawler.DefaultHeaders[k] = v
		}
	}
}

// SelfConfig 返回节点自身覆盖后的快照（供 API 展示）。
func (c *Config) SelfConfig() map[string]interface{} {
	return map[string]interface{}{
		"role":          c.Cluster.Role,
		"node_id":       c.Cluster.NodeID,
		"node_name":     c.Cluster.NodeName,
		"weight":        c.Cluster.Weight,
		"crawler_mode":  c.Crawler.Mode,
		"crawler_on":    c.Crawler.Enabled,
		"proxy_group":   c.Crawler.ProxyGroup,
		"headers":       c.Crawler.DefaultHeaders,
		"max_db_docs":   c.Limits.MaxDBDocsPerNode,
		"max_redis_docs": c.Limits.MaxRedisDocs,
	}
}

// RuntimeConfig 可在管理后台热更新的配置子集。
type RuntimeConfig struct {
	Search         *SearchConfig     `json:"search,omitempty"`
	Limits         *LimitsConfig     `json:"limits,omitempty"`
	CrawlerMode    *string           `json:"crawler_mode,omitempty"`
	ProxyGroup     *string           `json:"proxy_group,omitempty"`
	CrawlerEnabled *bool             `json:"crawler_enabled,omitempty"`
	DefaultHeaders map[string]string `json:"default_headers,omitempty"`
}

func (c *Config) ApplyRuntimeJSON(s string) error {
	if s == "" {
		return nil
	}
	var rc RuntimeConfig
	if err := json.Unmarshal([]byte(s), &rc); err != nil {
		return err
	}
	if rc.Search != nil {
		c.Search = *rc.Search
	}
	if rc.Limits != nil {
		c.Limits = *rc.Limits
	}
	if rc.CrawlerMode != nil {
		c.Crawler.Mode = *rc.CrawlerMode
	}
	if rc.ProxyGroup != nil {
		c.Crawler.ProxyGroup = *rc.ProxyGroup
	}
	if rc.CrawlerEnabled != nil {
		c.Crawler.Enabled = *rc.CrawlerEnabled
	}
	if len(rc.DefaultHeaders) > 0 {
		c.Crawler.DefaultHeaders = rc.DefaultHeaders
	}
	return nil
}

func (c *Config) RuntimeJSON() string {
	rc := RuntimeConfig{
		Search: &c.Search, Limits: &c.Limits,
		CrawlerMode: &c.Crawler.Mode, ProxyGroup: &c.Crawler.ProxyGroup,
		CrawlerEnabled: &c.Crawler.Enabled, DefaultHeaders: c.Crawler.DefaultHeaders,
	}
	b, _ := json.Marshal(rc)
	return string(b)
}
