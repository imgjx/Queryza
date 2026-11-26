package store

import "time"

type Document struct {
	ID            int64     `json:"id"`
	URL           string    `json:"url"`
	URLHash       string    `json:"url_hash"`
	Domain        string    `json:"domain"`
	Title         string    `json:"title"`
	Content       string    `json:"content"`
	ContentLen    int       `json:"content_len"`
	HTTPStatus    int       `json:"http_status"`
	FetchedAt     time.Time `json:"fetched_at"`
	ContentHash   string    `json:"content_hash"`
	SimHash       int64     `json:"simhash"`
	Embedding     []float32 `json:"-"`
	EmbeddingDim  int       `json:"embedding_dim"`
	SiteWeight    float64   `json:"site_weight"`
	DynamicWeight float64   `json:"dynamic_weight"`
	TrustScore    float64   `json:"trust_score"`
	SpamScore     float64   `json:"spam_score"`
	IsHot         bool      `json:"is_hot"`
	IndexStatus   int       `json:"index_status"`
	CreatedAt     time.Time `json:"created_at"`
}

type Seed struct {
	ID       int64   `json:"id"`
	URL      string  `json:"url"`
	Domain   string  `json:"domain"`
	Name     string  `json:"name"`
	Category string  `json:"category"`
	Weight   float64 `json:"weight"`
	Enabled  bool    `json:"enabled"`
	Depth    int     `json:"depth"`
}

type Node struct {
	ID            int64     `json:"-"`
	NodeID        string    `json:"node_id"`
	Name          string    `json:"name"`
	Addr          string    `json:"addr"`
	Role          string    `json:"role"`
	Status        string    `json:"status"`
	Weight        float64   `json:"weight"`
	DocCount      int64     `json:"doc_count"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	IsSelf        bool      `json:"is_self"`
	Config        string    `json:"config"`
	Secret        string    `json:"-"`
	Enabled       bool      `json:"enabled"`
	Version       string    `json:"version"`
	LastIP        string    `json:"last_ip"`
}

type DomainReplica struct {
	Domain string  `json:"domain"`
	NodeID string  `json:"node_id"`
	Role   string  `json:"role"`
	Weight float64 `json:"weight"`
}

type DomainAssignment struct {
	Domain string  `json:"domain"`
	NodeID string  `json:"node_id"`
	Role   string  `json:"role"`
	Weight float64 `json:"weight"`
}

type APIKey struct {
	ID         int64      `json:"id"`
	KeyPrefix  string     `json:"key_prefix"`
	Name       string     `json:"name"`
	QPSLimit   int        `json:"qps_limit"`
	DailyQuota int        `json:"daily_quota"`
	DailyUsed  int        `json:"daily_used"`
	ResultMax  int        `json:"result_max"`
	Enabled    bool       `json:"enabled"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
}

type CrawlTask struct {
	ID        int64     `json:"id"`
	URL       string    `json:"url"`
	URLHash   string    `json:"url_hash"`
	Domain    string    `json:"domain"`
	Depth     int       `json:"depth"`
	Priority  int       `json:"priority"`
	Status    string    `json:"status"`
	Retries   int       `json:"retries"`
	NodeID    string    `json:"node_id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CrawlLog struct {
	ID         int64     `json:"id"`
	URL        string    `json:"url"`
	Domain     string    `json:"domain"`
	Status     string    `json:"status"`
	HTTPCode   int       `json:"http_code"`
	Bytes      int       `json:"bytes"`
	Error      string    `json:"error"`
	DurationMS int       `json:"duration_ms"`
	NodeID     string    `json:"node_id"`
	RuleID     string    `json:"rule_id"`
	CreatedAt  time.Time `json:"created_at"`
}

type SearchHit struct {
	DocID      int64   `json:"doc_id"`
	URL        string  `json:"url"`
	Title      string  `json:"title"`
	Domain     string  `json:"domain"`
	Snippet    string  `json:"snippet"`
	Score      float64 `json:"score"`
	BM25       float64 `json:"bm25"`
	Vector     float64 `json:"vector"`
	SiteWeight float64 `json:"site_weight"`
	FetchedAt  string  `json:"fetched_at"`
	Source     string  `json:"source"` // keyword|vector|hybrid
}
