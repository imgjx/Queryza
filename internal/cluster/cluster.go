package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"queryza/internal/config"
	"queryza/internal/search"
	"queryza/internal/store"
)

type InternalSearchReq struct {
	Query string `json:"query"`
	TopK  int    `json:"top_k"`
}

type InternalSearchResp struct {
	OK     bool           `json:"ok"`
	Err    string         `json:"err"`
	Result *search.Result `json:"result"`
}

type RegisterReq struct {
	NodeID   string  `json:"node_id"`
	Name     string  `json:"name"`
	Addr     string  `json:"addr"`
	Role     string  `json:"role"`
	Weight   float64 `json:"weight"`
	DocCount int64   `json:"doc_count"`
	Config   string  `json:"config"`
}

// Master 中心调度：节点注册、心跳、域名分配、搜索 fan-out 与结果合并。
type Master struct {
	cfg      *config.Config
	db       *store.DB
	local    *search.Searcher
	client   *http.Client
	mu       sync.RWMutex
	nodes    map[string]*store.Node
}

func NewMaster(cfg *config.Config, db *store.DB, local *search.Searcher) *Master {
	m := &Master{
		cfg:    cfg,
		db:     db,
		local:  local,
		client: &http.Client{Timeout: 15 * time.Second},
		nodes:  map[string]*store.Node{},
	}
	m.loadNodes()
	return m
}

func (m *Master) loadNodes() {
	rows, err := m.db.Query(`SELECT node_id,name,addr,role,status,weight,doc_count,last_heartbeat,is_self,COALESCE(config,'')
		FROM nodes`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		n := &store.Node{}
		var hb *time.Time
		if rows.Scan(&n.NodeID, &n.Name, &n.Addr, &n.Role, &n.Status, &n.Weight, &n.DocCount, &hb, &n.IsSelf, &n.Config) == nil {
			if hb != nil {
				n.LastHeartbeat = *hb
			}
			m.nodes[n.NodeID] = n
		}
	}
}

func (m *Master) Register(req RegisterReq) {
	if req.Weight <= 0 {
		req.Weight = 1
	}
	if req.Role == "" {
		req.Role = "node"
	}
	isSelf := req.NodeID == m.cfg.Cluster.NodeID
	now := time.Now()
	_, err := m.db.Exec(`INSERT INTO nodes(node_id,name,addr,role,status,weight,doc_count,last_heartbeat,is_self,config)
		VALUES(?,?,?,?,?,?,?,?,?,?)
		ON DUPLICATE KEY UPDATE name=VALUES(name),addr=VALUES(addr),role=VALUES(role),status='online',
		weight=VALUES(weight),doc_count=VALUES(doc_count),last_heartbeat=NOW(),config=VALUES(config)`,
		req.NodeID, req.Name, req.Addr, req.Role, "online", req.Weight, req.DocCount, now, isSelf, req.Config)
	if err != nil {
		return
	}
	m.mu.Lock()
	m.nodes[req.NodeID] = &store.Node{
		NodeID: req.NodeID, Name: req.Name, Addr: req.Addr, Role: req.Role, Status: "online",
		Weight: req.Weight, DocCount: req.DocCount, LastHeartbeat: now, IsSelf: isSelf, Config: req.Config,
	}
	m.mu.Unlock()
}

func (m *Master) Heartbeat(nodeID string, docCount int64, cfgStr string) {
	now := time.Now()
	m.db.Exec(`UPDATE nodes SET status='online',last_heartbeat=NOW(),doc_count=?,config=? WHERE node_id=?`, docCount, cfgStr, nodeID)
	m.mu.Lock()
	if n, ok := m.nodes[nodeID]; ok {
		n.Status = "online"
		n.LastHeartbeat = now
		n.DocCount = docCount
		if cfgStr != "" {
			n.Config = cfgStr
		}
	}
	m.mu.Unlock()
}

func (m *Master) ListNodes() []store.Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]store.Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		n.Status = m.statusOf(n)
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func (m *Master) statusOf(n *store.Node) string {
	if n.IsSelf {
		return "online"
	}
	ttl := time.Duration(m.cfg.Cluster.HeartbeatSec*3) * time.Second
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if time.Since(n.LastHeartbeat) > ttl {
		return "offline"
	}
	return "online"
}

func (m *Master) OnlineNodes() []store.Node {
	var out []store.Node
	for _, n := range m.ListNodes() {
		if n.Status == "online" {
			out = append(out, n)
		}
	}
	return out
}

// AssignDomain 为域名选择节点（域名最少者优先），并持久化。
func (m *Master) AssignDomain(domain string) string {
	var nodeID string
	m.db.QueryRow(`SELECT node_id FROM domain_assignments WHERE domain=?`, domain).Scan(&nodeID)
	if nodeID != "" {
		return nodeID
	}
	nodes := m.OnlineNodes()
	if len(nodes) == 0 {
		return ""
	}
	counts := map[string]int{}
	rows, _ := m.db.Query(`SELECT node_id, COUNT(*) FROM domain_assignments GROUP BY node_id`)
	if rows != nil {
		for rows.Next() {
			var id string
			var c int
			rows.Scan(&id, &c)
			counts[id] = c
		}
		rows.Close()
	}
	best := nodes[0].NodeID
	bestScore := 1 << 30
	for _, n := range nodes {
		score := counts[n.NodeID]
		if score < bestScore {
			bestScore = score
			best = n.NodeID
		}
	}
	m.db.Exec(`INSERT IGNORE INTO domain_assignments(domain,node_id) VALUES(?,?)`, domain, best)
	return best
}

func (m *Master) Assignments() []store.DomainAssignment {
	rows, err := m.db.Query(`SELECT domain,node_id,weight FROM domain_assignments ORDER BY domain`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []store.DomainAssignment
	for rows.Next() {
		var a store.DomainAssignment
		if rows.Scan(&a.Domain, &a.NodeID, &a.Weight) == nil {
			out = append(out, a)
		}
	}
	return out
}

type nodeResult struct {
	node store.Node
	res  *search.Result
	err  error
}

// Search 向在线节点 fan-out，归一化后按节点权重合并去重。
func (m *Master) Search(ctx context.Context, query string, topK int) (*search.Result, error) {
	start := time.Now()
	nodes := m.OnlineNodes()
	if len(nodes) == 0 {
		return m.local.SearchLocal(ctx, query, topK)
	}
	// 单节点（standalone / 仅本机）直连，保留原始 BM25 分与分词结果
	if len(nodes) == 1 && nodes[0].NodeID == m.cfg.Cluster.NodeID {
		return m.local.SearchLocal(ctx, query, topK)
	}

	results := make([]nodeResult, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, n store.Node) {
			defer wg.Done()
			if n.NodeID == m.cfg.Cluster.NodeID {
				r, err := m.local.SearchLocal(ctx, query, topK)
				results[i] = nodeResult{node: n, res: r, err: err}
				return
			}
			r, err := m.remoteSearch(ctx, n.Addr, query, topK)
			results[i] = nodeResult{node: n, res: r, err: err}
		}(i, n)
	}
	wg.Wait()

	best := map[string]store.SearchHit{}
	var tokens []string
	for _, nr := range results {
		if len(tokens) == 0 && nr.res != nil && len(nr.res.Tokens) > 0 {
			tokens = nr.res.Tokens
		}
		if nr.err != nil || nr.res == nil || len(nr.res.Hits) == 0 {
			continue
		}
		norm := normalizeHits(nr.res.Hits)
		w := nr.node.Weight
		if w <= 0 {
			w = 1
		}
		for i, h := range nr.res.Hits {
			h.Score = norm[i] * w
			if old, ok := best[h.URL]; !ok || h.Score > old.Score {
				best[h.URL] = h
			}
		}
	}

	hits := make([]store.SearchHit, 0, len(best))
	for _, h := range best {
		hits = append(hits, h)
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
	total := len(hits)
	if topK > 0 && len(hits) > topK {
		hits = hits[:topK]
	}
	return &search.Result{
		Query: query, Tokens: tokens, Mode: "cluster", Total: total,
		TookMS: int(time.Since(start).Milliseconds()), Hits: hits, NodeID: "master",
	}, nil
}

func normalizeHits(hits []store.SearchHit) []float64 {
	out := make([]float64, len(hits))
	if len(hits) == 0 {
		return out
	}
	mn, mx := hits[len(hits)-1].Score, hits[0].Score
	for _, h := range hits {
		if h.Score < mn {
			mn = h.Score
		}
		if h.Score > mx {
			mx = h.Score
		}
	}
	span := mx - mn
	for i, h := range hits {
		if span <= 1e-12 {
			out[i] = 1
		} else {
			out[i] = (h.Score - mn) / span
		}
	}
	return out
}

func (m *Master) remoteSearch(ctx context.Context, addr, query string, topK int) (*search.Result, error) {
	body, _ := json.Marshal(InternalSearchReq{Query: query, TopK: topK})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(addr, "/internal/search"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	var ir InternalSearchResp
	if err := json.Unmarshal(b, &ir); err != nil {
		return nil, err
	}
	if !ir.OK {
		return nil, fmt.Errorf("节点返回错误: %s", ir.Err)
	}
	return ir.Result, nil
}

func joinURL(base, path string) string {
	for len(base) > 0 && base[len(base)-1] == '/' {
		base = base[:len(base)-1]
	}
	return base + path
}

// Agent 运行在子节点上，负责注册与心跳。
type Agent struct {
	cfg    *config.Config
	addr   string
	master string
	client *http.Client
}

func NewAgent(cfg *config.Config, advertiseAddr string) *Agent {
	return &Agent{cfg: cfg, addr: advertiseAddr, master: cfg.Cluster.MasterAddr, client: &http.Client{Timeout: 5 * time.Second}}
}

func (a *Agent) Register(ctx context.Context, docCount int64, cfgStr string) error {
	return a.post(ctx, "/internal/register", RegisterReq{
		NodeID: a.cfg.Cluster.NodeID, Name: a.cfg.Cluster.NodeName, Addr: a.addr,
		Role: a.cfg.Cluster.Role, Weight: a.cfg.Cluster.Weight, DocCount: docCount, Config: cfgStr,
	})
}

func (a *Agent) Heartbeat(ctx context.Context, docCount int64, cfgStr string) error {
	return a.post(ctx, "/internal/heartbeat", RegisterReq{
		NodeID: a.cfg.Cluster.NodeID, DocCount: docCount, Config: cfgStr,
	})
}

func (a *Agent) post(ctx context.Context, path string, payload interface{}) error {
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(a.master, path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("master 返回 %d", resp.StatusCode)
	}
	return nil
}
