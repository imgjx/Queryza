package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"queryza/internal/config"
	"queryza/internal/search"
	"queryza/internal/store"
)

type InternalSearchReq struct {
	Query string `json:"query"`
	TopK  int    `json:"top_k"`
	// Domains 非空时按指定域名集合检索（主节点在副本上代查故障节点负责的域名）
	Domains []string `json:"domains,omitempty"`
}

// InternalDocsReq 主节点向子节点拉取文档库分页数据。
type InternalDocsReq struct {
	Page   int    `json:"page"`
	Size   int    `json:"size"`
	Q      string `json:"q"`
	Domain string `json:"domain"`
	Shard  *int   `json:"shard"` // nil 表示不限分片
}

type InternalDocsResp struct {
	OK         bool                 `json:"ok"`
	Err        string               `json:"err"`
	Docs       []store.DocRow       `json:"docs"`
	Total      int64                `json:"total"`
	Page       int                  `json:"page"`
	Size       int                  `json:"size"`
	Stats      *store.DocStats      `json:"stats"`
	TopDomains []store.DomainCount  `json:"top_domains"`
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
	Version  string  `json:"version"`
	Token    string  `json:"token"`
	IP       string  `json:"-"`
}

type EnrollResp struct {
	OK       bool   `json:"ok"`
	Err      string `json:"err"`
	NodeID   string `json:"node_id"`
	Secret   string `json:"secret"`
	MasterID string `json:"master_id"`
}

// Master 中心调度：节点注册、心跳、域名多副本分配、检索路由与结果合并。
type Master struct {
	cfg    *config.Config
	db     *store.DB
	local  *search.Searcher
	client *http.Client

	mu    sync.RWMutex
	nodes map[string]*store.Node

	// asgMu 保护域名主副本缓存：抓取热路径上每条 URL 都要判定归属，
	// 必须走内存，不能每次查库（曾是最严重的串行点）。
	asgMu    sync.RWMutex
	asgCache map[string]*asgEntry

	// 域名分片数缓存
	shardMu sync.RWMutex
	shards  map[string]int
	shardAt time.Time
	assignMu sync.Mutex

	domMu   sync.Mutex
	domains map[string][]string // nodeID -> 主副本域名
	domAt   time.Time
}

type asgEntry struct {
	primary string
	at      time.Time
	ttl     time.Duration
}

func NewMaster(cfg *config.Config, db *store.DB, local *search.Searcher) *Master {
	m := &Master{
		cfg:      cfg,
		db:       db,
		local:    local,
		client:   &http.Client{Timeout: 15 * time.Second},
		nodes:    map[string]*store.Node{},
		asgCache: map[string]*asgEntry{},
	}
	m.loadNodes()
	m.ensureSelf()
	return m
}

func (m *Master) loadNodes() {
	rows, err := m.db.Query(`SELECT node_id,name,addr,role,status,weight,doc_count,last_heartbeat,is_self,
		COALESCE(config,''),COALESCE(secret,''),enabled,COALESCE(version,''),COALESCE(last_ip,'')
		FROM nodes`)
	if err != nil {
		return
	}
	defer rows.Close()
	next := map[string]*store.Node{}
	for rows.Next() {
		n := &store.Node{}
		var hb *time.Time
		if rows.Scan(&n.NodeID, &n.Name, &n.Addr, &n.Role, &n.Status, &n.Weight, &n.DocCount, &hb,
			&n.IsSelf, &n.Config, &n.Secret, &n.Enabled, &n.Version, &n.LastIP) == nil {
			if hb != nil {
				n.LastHeartbeat = *hb
			}
			next[n.NodeID] = n
		}
	}
	m.mu.Lock()
	m.nodes = next
	m.mu.Unlock()
}

// ensureSelf 保证本机节点在库中存在并持有密钥。
func (m *Master) ensureSelf() {
	id := m.cfg.Cluster.NodeID
	// 子节点身份由主节点下发，不能在本地自造密钥
	if id == "" || m.cfg.Cluster.Role == "node" {
		return
	}
	secret := m.cfg.Cluster.Secret
	var existing string
	m.db.QueryRow(`SELECT secret FROM nodes WHERE node_id=?`, id).Scan(&existing)
	if existing == "" {
		if secret == "" {
			secret = RandToken(24)
		}
		m.db.Exec(`INSERT INTO nodes(node_id,name,addr,role,status,weight,doc_count,last_heartbeat,is_self,config,secret,enabled)
			VALUES(?,?,?,?,'online',?,0,NOW(),1,'',?,1)
			ON DUPLICATE KEY UPDATE secret=VALUES(secret),enabled=1`,
			id, m.cfg.Cluster.NodeName, m.cfg.Cluster.AdvertiseAddr, m.cfg.Cluster.Role, m.cfg.Cluster.Weight, secret)
		existing = secret
	}
	m.cfg.Cluster.Secret = existing
	// 历史记录里可能残留旧节点的 is_self=1，会让它绕过心跳检查被当成永远在线，
	// 进而把域名分给一个已经下线的节点（抓取任务永远 pending）
	m.db.Exec(`UPDATE nodes SET is_self=0 WHERE node_id<>? AND is_self=1`, id)
	m.loadNodes()
}

// SelfID 返回本节点 ID。
func (m *Master) SelfID() string { return m.cfg.Cluster.NodeID }

// NodeSecret 返回节点密钥，供签名校验使用。
func (m *Master) NodeSecret(nodeID string) (string, error) {
	m.mu.RLock()
	n, ok := m.nodes[nodeID]
	m.mu.RUnlock()
	if !ok || !n.Enabled {
		return "", ErrNodeDenied
	}
	return n.Secret, nil
}

// Enroll 用一次性令牌换取节点密钥。
func (m *Master) Enroll(token string) (string, string, error) {
	if token == "" {
		return "", "", errors.New("加入令牌为空")
	}
	var nodeID string
	var expires time.Time
	var used bool
	if err := m.db.QueryRow(`SELECT node_id,expires_at,used FROM node_tokens WHERE token=?`, token).
		Scan(&nodeID, &expires, &used); err != nil {
		return "", "", errors.New("加入令牌无效")
	}
	if used {
		return "", "", errors.New("加入令牌已被使用")
	}
	if time.Now().After(expires) {
		return "", "", errors.New("加入令牌已过期")
	}
	m.mu.RLock()
	n := m.nodes[nodeID]
	m.mu.RUnlock()
	if n == nil {
		return "", "", errors.New("节点不存在")
	}
	secret := n.Secret
	if secret == "" {
		secret = RandToken(24)
		m.db.Exec(`UPDATE nodes SET secret=? WHERE node_id=?`, secret, nodeID)
	}
	m.db.Exec(`UPDATE node_tokens SET used=1 WHERE token=?`, token)
	m.db.Exec(`UPDATE nodes SET enabled=1 WHERE node_id=?`, nodeID)
	m.loadNodes()
	return nodeID, secret, nil
}

// Register 只接受已在管理后台创建、且签名校验通过的节点。
func (m *Master) Register(req RegisterReq) error {
	m.mu.RLock()
	n, ok := m.nodes[req.NodeID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %s", ErrNodeDenied, req.NodeID)
	}
	if !n.Enabled && req.NodeID != m.cfg.Cluster.NodeID {
		return fmt.Errorf("%w: 节点已禁用", ErrNodeDenied)
	}
	if req.Weight <= 0 {
		req.Weight = 1
	}
	if req.Role == "" {
		req.Role = "node"
	}
	// 节点未自定义名称时用节点 ID，避免全部显示成「本地节点」
	if req.Name == "" || req.Name == "本地节点" {
		req.Name = req.NodeID
	}
	isSelf := req.NodeID == m.cfg.Cluster.NodeID
	now := time.Now()
	_, err := m.db.Exec(`UPDATE nodes SET name=?,addr=?,role=?,status='online',weight=?,doc_count=?,
		last_heartbeat=NOW(),version=?,last_ip=? WHERE node_id=?`,
		req.Name, req.Addr, req.Role, req.Weight, req.DocCount, req.Version, req.IP, req.NodeID)
	if err != nil {
		return err
	}
	m.mu.Lock()
	if cur, ok := m.nodes[req.NodeID]; ok {
		cur.Name, cur.Addr, cur.Role = req.Name, req.Addr, req.Role
		cur.Status, cur.Weight, cur.DocCount = "online", req.Weight, req.DocCount
		cur.LastHeartbeat, cur.Version, cur.LastIP = now, req.Version, req.IP
		cur.IsSelf = isSelf
	} else {
		m.nodes[req.NodeID] = &store.Node{
			NodeID: req.NodeID, Name: req.Name, Addr: req.Addr, Role: req.Role, Status: "online",
			Weight: req.Weight, DocCount: req.DocCount, LastHeartbeat: now, IsSelf: isSelf, Enabled: true,
		}
	}
	m.mu.Unlock()
	if !isSelf {
		// 新节点上线后重新均衡域名归属
		go func() {
			time.Sleep(500 * time.Millisecond)
			m.Rebalance()
		}()
	}
	return nil
}

func (m *Master) Heartbeat(nodeID string, docCount int64, cfgStr, version, ip string) error {
	m.mu.RLock()
	n, ok := m.nodes[nodeID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %s", ErrNodeDenied, nodeID)
	}
	if !n.Enabled && nodeID != m.cfg.Cluster.NodeID {
		return fmt.Errorf("%w: 节点已禁用", ErrNodeDenied)
	}
	m.db.Exec(`UPDATE nodes SET status='online',last_heartbeat=NOW(),doc_count=?,config=?,version=?,last_ip=? WHERE node_id=?`,
		docCount, cfgStr, version, ip, nodeID)
	now := time.Now()
	m.mu.Lock()
	if cur, ok := m.nodes[nodeID]; ok {
		cur.Status, cur.LastHeartbeat, cur.DocCount = "online", now, docCount
		if cfgStr != "" {
			cur.Config = cfgStr
		}
		cur.Version, cur.LastIP = version, ip
	}
	m.mu.Unlock()
	return nil
}

func (m *Master) ListNodes() []store.Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]store.Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		c := *n
		c.Status = m.statusOf(n)
		// Secret 字段带 json:"-"，不会序列化出去，保留供内部调用签名
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func (m *Master) statusOf(n *store.Node) string {
	if !n.Enabled && !n.IsSelf {
		return "disabled"
	}
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
	timeout := time.Duration(m.cfg.Cluster.HeartbeatSec) * time.Second * 3
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	cutoff := time.Now().Add(-timeout)

	var out []store.Node
	for _, n := range m.ListNodes() {
		if n.Status != "online" {
			continue
		}
		if !n.IsSelf && (n.LastHeartbeat.IsZero() || n.LastHeartbeat.Before(cutoff)) {
			continue
		}
		out = append(out, n)
	}
	return out
}

// ---------- 域名分配（多副本） ----------

// OwnedDomains 返回本节点负责的域名，供抓取队列过滤。
func (m *Master) OwnedDomains() []string { return m.DomainsOf(m.cfg.Cluster.NodeID) }

func (m *Master) invalidateDomains() {
	m.invalidateDomainList()
	// 副本组变化（提主/再均衡/删节点）后，归属缓存必须同步失效
	m.asgMu.Lock()
	m.asgCache = map[string]*asgEntry{}
	m.asgMu.Unlock()
}

// invalidateDomainList 只让域名列表缓存过期，保留逐域名归属缓存。
func (m *Master) invalidateDomainList() {
	m.domMu.Lock()
	m.domAt = time.Time{}
	m.domMu.Unlock()
}

// DomainsOf 返回节点负责的主副本域名（5 秒缓存）。
func (m *Master) DomainsOf(nodeID string) []string {
	m.domMu.Lock()
	defer m.domMu.Unlock()
	if m.domains == nil || time.Since(m.domAt) > 5*time.Second {
		next := map[string][]string{}
		seen := map[string]map[string]bool{}
		rows, err := m.db.Query(`SELECT node_id,domain FROM domain_replicas WHERE role='primary'`)
		if err == nil {
			for rows.Next() {
				var id, d string
				if rows.Scan(&id, &d) == nil {
					// 同一域名的多个分片可能落在同一节点，列表里只保留一次
					if seen[id] == nil {
						seen[id] = map[string]bool{}
					}
					if seen[id][d] {
						continue
					}
					seen[id][d] = true
					next[id] = append(next[id], d)
				}
			}
			rows.Close()
		}
		m.domains, m.domAt = next, time.Now()
	}
	return m.domains[nodeID]
}

// SearchNodes 返回对检索有贡献的节点：每个域名只由主副本出结果，避免重复召回。
func (m *Master) SearchNodes() []store.Node {
	online := m.OnlineNodes()
	if len(online) <= 1 {
		return online
	}
	m.domMu.Lock()
	if m.domains == nil || time.Since(m.domAt) > 5*time.Second {
		m.domMu.Unlock()
		m.DomainsOf("")
		m.domMu.Lock()
	}
	owners := map[string]bool{}
	for id := range m.domains {
		owners[id] = true
	}
	m.domMu.Unlock()

	var out []store.Node
	for _, n := range online {
		if owners[n.NodeID] {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return online
	}
	return out
}

// BackfillAssignments 为已有文档的域名补建副本分配（启动时调用）。
func (m *Master) BackfillAssignments(ctx context.Context) int {
	if m.cfg.Cluster.Role != "master" && m.cfg.Cluster.Role != "standalone" {
		return 0
	}
	rows, err := m.db.QueryContext(ctx, `SELECT DISTINCT d.domain FROM documents d
		LEFT JOIN domain_replicas r ON r.domain=d.domain WHERE r.domain IS NULL LIMIT 5000`)
	if err != nil {
		return 0
	}
	var domains []string
	for rows.Next() {
		var d string
		if rows.Scan(&d) == nil {
			domains = append(domains, d)
		}
	}
	rows.Close()
	for _, d := range domains {
		m.AssignDomain(d)
	}
	return len(domains)
}

// Promote 主副本离线时把在线副本提为主，保证域名仍可检索。
func (m *Master) Promote() {
	online := map[string]bool{}
	for _, n := range m.OnlineNodes() {
		online[n.NodeID] = true
	}
	rows, err := m.db.Query(`SELECT domain,node_id,role FROM domain_replicas`)
	if err != nil {
		return
	}
	type item struct{ id, role string }
	group := map[string][]item{}
	for rows.Next() {
		var d, id, role string
		if rows.Scan(&d, &id, &role) == nil {
			group[d] = append(group[d], item{id, role})
		}
	}
	rows.Close()

	changed := false
	for domain, list := range group {
		var prim string
		var cand string
		for _, it := range list {
			if it.role == "primary" {
				prim = it.id
			}
		}
		if prim != "" && online[prim] {
			continue
		}
		for _, it := range list {
			if it.role == "replica" && online[it.id] {
				cand = it.id
				break
			}
		}
		if cand == "" {
			continue
		}
		m.db.Exec(`UPDATE domain_replicas SET role='replica' WHERE domain=? AND role='primary'`, domain)
		m.db.Exec(`UPDATE domain_replicas SET role='primary' WHERE domain=? AND node_id=?`, domain, cand)
		changed = true
	}
	if changed {
		m.invalidateDomains()
	}
}

func (m *Master) Assignments() []store.DomainAssignment {
	rows, err := m.db.Query(`SELECT domain,shard,node_id,role,weight FROM domain_replicas
		ORDER BY domain,shard,role DESC`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []store.DomainAssignment
	for rows.Next() {
		var a store.DomainAssignment
		if rows.Scan(&a.Domain, &a.Shard, &a.NodeID, &a.Role, &a.Weight) == nil {
			out = append(out, a)
		}
	}
	return out
}

// ---------- 检索 ----------

type nodeResult struct {
	node store.Node
	res  *search.Result
	err  error
}

// Search 向「主副本节点」fan-out，合并后去重。
func (m *Master) Search(ctx context.Context, query string, topK, offset int) (*search.Result, error) {
	start := time.Now()
	if offset < 0 {
		offset = 0
	}
	// 各节点都要多返回 offset 条，才能在合并排序后切出第 N 页
	ask := topK + offset
	nodes := m.SearchNodes()
	if len(nodes) == 0 {
		r, err := m.local.SearchLocal(ctx, query, ask)
		return sliceResult(r, topK, offset), err
	}
	// 单节点直接走本地，保留原始 BM25 分与分词结果
	if len(nodes) == 1 && nodes[0].NodeID == m.cfg.Cluster.NodeID {
		r, err := m.local.SearchLocal(ctx, query, ask)
		return sliceResult(r, topK, offset), err
	}

	results := make([]nodeResult, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, n store.Node) {
			defer wg.Done()
			if n.NodeID == m.cfg.Cluster.NodeID {
				allow := m.domainSet(m.DomainsOf(n.NodeID))
				r, err := m.local.SearchLocalWithin(ctx, query, ask, allow)
				results[i] = nodeResult{node: n, res: r, err: err}
				return
			}
			r, err := m.remoteSearch(ctx, n, query, ask)
			results[i] = nodeResult{node: n, res: r, err: err}
		}(i, n)
	}
	wg.Wait()

	best := map[string]store.SearchHit{}
	var tokens []string
	var failed []string
	for _, nr := range results {
		if len(tokens) == 0 && nr.res != nil && len(nr.res.Tokens) > 0 {
			tokens = nr.res.Tokens
		}
		if nr.err != nil {
			// 主副本不可达：找一个持有该域名副本的在线节点代查
			if r, via, err := m.replicaSearch(ctx, nr.node.NodeID, query, ask); err == nil && r != nil {
				nr.res, nr.node.NodeID, nr.node.Weight = r, via.NodeID, via.Weight
				if len(tokens) == 0 && len(r.Tokens) > 0 {
					tokens = r.Tokens
				}
			} else {
				failed = append(failed, nr.node.NodeID+": "+nr.err.Error())
				continue
			}
		}
		if nr.res == nil || len(nr.res.Hits) == 0 {
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
	if offset >= len(hits) {
		hits = []store.SearchHit{}
	} else {
		hits = hits[offset:]
		if topK > 0 && len(hits) > topK {
			hits = hits[:topK]
		}
	}
	res := &search.Result{
		Query: query, Tokens: tokens, Mode: "cluster", Total: total,
		TookMS: int(time.Since(start).Milliseconds()), Hits: hits, NodeID: "master",
	}
	if len(failed) > 0 {
		res.Degraded = failed
	}
	return res, nil
}

// BroadcastCrawl 把抓取开关下发给所有在线节点。
func (m *Master) BroadcastCrawl(ctx context.Context, action string) []string {
	var failed []string
	for _, n := range m.OnlineNodes() {
		if n.NodeID == m.cfg.Cluster.NodeID || n.Addr == "" {
			continue
		}
		body, _ := json.Marshal(map[string]string{"action": action})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(n.Addr, "/internal/crawl"), bytes.NewReader(body))
		if err != nil {
			failed = append(failed, n.NodeID)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		SignRequest(req, m.cfg.Cluster.Secret, m.cfg.Cluster.NodeID, body)
		resp, err := m.client.Do(req)
		if err != nil {
			failed = append(failed, n.NodeID+": "+err.Error())
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			failed = append(failed, n.NodeID+fmt.Sprintf(": %d", resp.StatusCode))
		}
	}
	return failed
}

// replicaSearch 在副本节点上代查故障节点负责的域名。
func (m *Master) replicaSearch(ctx context.Context, failedNode, query string, topK int) (*search.Result, store.Node, error) {
	domains := m.DomainsOf(failedNode)
	if len(domains) == 0 || len(domains) > 300 {
		return nil, store.Node{}, errors.New("无需代查")
	}
	marks := make([]string, len(domains))
	for i := range domains {
		marks[i] = "?"
	}
	args := make([]interface{}, 0, len(domains)+1)
	for _, d := range domains {
		args = append(args, d)
	}
	args = append(args, failedNode)
	row := m.db.QueryRowContext(ctx, `SELECT node_id FROM domain_replicas
		WHERE domain IN (`+strings.Join(marks, ",")+`) AND node_id<>? GROUP BY node_id ORDER BY COUNT(*) DESC LIMIT 1`, args...)
	var replicaID string
	if err := row.Scan(&replicaID); err != nil || replicaID == "" {
		return nil, store.Node{}, errors.New("没有可用副本")
	}
	var target *store.Node
	m.mu.RLock()
	target = m.nodes[replicaID]
	m.mu.RUnlock()
	if target == nil {
		return nil, store.Node{}, errors.New("副本节点不存在")
	}
	if replicaID == m.cfg.Cluster.NodeID {
		r, err := m.local.SearchLocalWithin(ctx, query, topK, m.domainSet(domains))
		return r, *target, err
	}
	r, err := m.remoteSearchDomains(ctx, *target, query, topK, domains)
	return r, *target, err
}

// NodeDocs 拉取指定节点的文档库分页数据；nodeID 为空或本节点时直接查本地库。
func (m *Master) NodeDocs(ctx context.Context, nodeID string, q store.DocQuery) (*InternalDocsResp, error) {
	if nodeID == "" || nodeID == m.cfg.Cluster.NodeID {
		docs, total, err := m.db.QueryDocs(ctx, q)
		if err != nil {
			return nil, err
		}
		st := m.db.Stats(ctx)
		return &InternalDocsResp{
			OK: true, Docs: docs, Total: total, Page: q.Page, Size: q.Size,
			Stats: &st, TopDomains: m.db.TopDomains(ctx, 12),
		}, nil
	}

	m.mu.RLock()
	node := m.nodes[nodeID]
	m.mu.RUnlock()
	if node == nil {
		return nil, errors.New("节点不存在")
	}
	if node.Secret == "" {
		return nil, errors.New("节点未持有密钥")
	}
	var shard *int
	if q.Shard >= 0 {
		v := q.Shard
		shard = &v
	}
	body, _ := json.Marshal(InternalDocsReq{Page: q.Page, Size: q.Size, Q: q.Q, Domain: q.Domain, Shard: shard})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(node.Addr, "/internal/docs"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	SignRequest(req, m.cfg.Cluster.Secret, m.cfg.Cluster.NodeID, body)
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("节点返回 %d", resp.StatusCode)
	}
	var ir InternalDocsResp
	if err := json.Unmarshal(b, &ir); err != nil {
		return nil, err
	}
	if !ir.OK {
		return nil, fmt.Errorf("节点错误: %s", ir.Err)
	}
	return &ir, nil
}

// sliceResult 从已按分数排序的结果里切出第 N 页（本地路径用）。
func sliceResult(r *search.Result, topK, offset int) *search.Result {
	if r == nil {
		return nil
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(r.Hits) {
		r.Hits = []store.SearchHit{}
		return r
	}
	r.Hits = r.Hits[offset:]
	if topK > 0 && len(r.Hits) > topK {
		r.Hits = r.Hits[:topK]
	}
	return r
}

func (m *Master) domainSet(list []string) map[string]bool {
	if len(list) == 0 {
		return nil
	}
	out := make(map[string]bool, len(list))
	for _, d := range list {
		out[d] = true
	}
	return out
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

func (m *Master) remoteSearch(ctx context.Context, node store.Node, query string, topK int) (*search.Result, error) {
	return m.remoteSearchDomains(ctx, node, query, topK, nil)
}

func (m *Master) remoteSearchDomains(ctx context.Context, node store.Node, query string, topK int, domains []string) (*search.Result, error) {
	if node.Secret == "" {
		return nil, errors.New("节点未持有密钥")
	}
	body, _ := json.Marshal(InternalSearchReq{Query: query, TopK: topK, Domains: domains})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(node.Addr, "/internal/search"), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	// 用自己的密钥签名：接收方按 Node 头从库里查到主节点密钥后校验
	SignRequest(req, m.cfg.Cluster.Secret, m.cfg.Cluster.NodeID, body)
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("节点返回 %d", resp.StatusCode)
	}
	var ir InternalSearchResp
	if err := json.Unmarshal(b, &ir); err != nil {
		return nil, err
	}
	if !ir.OK {
		return nil, fmt.Errorf("节点错误: %s", ir.Err)
	}
	return ir.Result, nil
}

func joinURL(base, path string) string {
	for len(base) > 0 && base[len(base)-1] == '/' {
		base = base[:len(base)-1]
	}
	return base + path
}

// ---------- 节点管理（后台） ----------

func (m *Master) CreateNode(nodeID, name, addr string, weight float64) (string, string, error) {
	if nodeID == "" {
		nodeID = "node-" + RandToken(3)
	}
	if weight <= 0 {
		weight = 1
	}
	secret := RandToken(24)
	_, err := m.db.Exec(`INSERT INTO nodes(node_id,name,addr,role,status,weight,enabled,secret,config)
		VALUES(?,?,?,'node','pending',?,0,?,'')
		ON DUPLICATE KEY UPDATE name=VALUES(name),addr=VALUES(addr),weight=VALUES(weight)`,
		nodeID, name, addr, weight, secret)
	if err != nil {
		return "", "", err
	}
	token := RandToken(24)
	if _, err := m.db.Exec(`INSERT INTO node_tokens(token,node_id,expires_at)
		VALUES(?,?,DATE_ADD(NOW(), INTERVAL 24 HOUR))`, token, nodeID); err != nil {
		return "", "", err
	}
	m.loadNodes()
	return nodeID, token, nil
}

func (m *Master) DeleteNode(nodeID string) error {
	if nodeID == m.cfg.Cluster.NodeID {
		return errors.New("不能删除当前主节点")
	}
	m.db.Exec(`DELETE FROM nodes WHERE node_id=?`, nodeID)
	m.db.Exec(`DELETE FROM node_tokens WHERE node_id=?`, nodeID)
	m.db.Exec(`DELETE FROM domain_replicas WHERE node_id=?`, nodeID)
	m.loadNodes()
	m.invalidateDomains()
	return nil
}

func (m *Master) ResetNodeSecret(nodeID string) (string, error) {
	m.mu.RLock()
	n, ok := m.nodes[nodeID]
	m.mu.RUnlock()
	if !ok {
		return "", errors.New("节点不存在")
	}
	if n.IsSelf {
		return "", errors.New("请在配置文件中修改本机密钥")
	}
	secret := RandToken(24)
	m.db.Exec(`UPDATE nodes SET secret=?,enabled=0,status='pending' WHERE node_id=?`, secret, nodeID)
	token := RandToken(24)
	m.db.Exec(`INSERT INTO node_tokens(token,node_id,expires_at) VALUES(?,?,DATE_ADD(NOW(), INTERVAL 24 HOUR))`, token, nodeID)
	m.loadNodes()
	return token, nil
}

func (m *Master) SetNodeWeight(nodeID string, weight float64) error {
	if weight <= 0 {
		weight = 1
	}
	if _, err := m.db.Exec(`UPDATE nodes SET weight=? WHERE node_id=?`, weight, nodeID); err != nil {
		return err
	}
	m.mu.Lock()
	if n, ok := m.nodes[nodeID]; ok {
		n.Weight = weight
	}
	m.mu.Unlock()
	return nil
}

// refreshSelfCount 统计本节点负责的文档数，供后台展示。
func (m *Master) refreshSelfCount(ctx context.Context) {
	var n int64
	m.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents d
		JOIN domain_replicas r ON r.domain=d.domain AND r.node_id=? AND r.role='primary'`,
		m.cfg.Cluster.NodeID).Scan(&n)
	m.db.ExecContext(ctx, `UPDATE nodes SET doc_count=?,last_ip=COALESCE(NULLIF(last_ip,''),?) WHERE node_id=?`,
		n, hostPart(m.cfg.Cluster.AdvertiseAddr), m.cfg.Cluster.NodeID)
	m.mu.Lock()
	if node, ok := m.nodes[m.cfg.Cluster.NodeID]; ok {
		node.DocCount = n
		if node.LastIP == "" {
			node.LastIP = hostPart(m.cfg.Cluster.AdvertiseAddr)
		}
	}
	m.mu.Unlock()
}

// StartMaintenance 周期性地做副本提主与离线标记。
func (m *Master) StartMaintenance(ctx context.Context) {
	interval := time.Duration(m.cfg.Cluster.HeartbeatSec) * time.Second
	if interval <= 0 {
		interval = 10 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	tick := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick++
			m.Promote()
			// 每 6 个心跳做一次再均衡：太频繁会在节点间来回搬分片
			if tick%6 == 0 {
				if n := m.Rebalance(); n > 0 {
					log.Printf("分配再均衡：迁移 %d 个分片", n)
				}
			}
			// 每 5 分钟按文档量检查一次是否需要加分片
			if m.cfg.Cluster.AutoShard && tick%30 == 0 {
				if n := m.EnsureAutoShards(m.cfg.Cluster.ShardDocsPerNode); n > 0 {
					log.Printf("自动分片：调整了 %d 个域名", n)
				}
			}
			m.refreshSelfCount(ctx)
		}
	}
}

// ---------- 子节点 Agent ----------

type identity struct {
	NodeID string `json:"node_id"`
	Secret string `json:"secret"`
	Master string `json:"master"`
}

// Agent 运行在子节点上：按主地址表顺序连接，失败自动切换。
type Agent struct {
	cfg    *config.Config
	addr   string
	client *http.Client

	mu      sync.Mutex
	masters []string
	cur     int
	secret  string
	nodeID  string
	token   string
}

func NewAgent(cfg *config.Config, advertiseAddr string) *Agent {
	a := &Agent{
		cfg:     cfg,
		addr:    advertiseAddr,
		client:  &http.Client{Timeout: 8 * time.Second},
		masters: append([]string{}, cfg.Cluster.MasterAddrs...),
		secret:  cfg.Cluster.Secret,
		nodeID:  cfg.Cluster.NodeID,
		token:   cfg.Cluster.Token,
	}
	a.loadIdentity()
	return a
}

func (a *Agent) loadIdentity() {
	if a.cfg.Cluster.IdentityFile == "" {
		return
	}
	b, err := readFile(a.cfg.Cluster.IdentityFile)
	if err != nil {
		return
	}
	var id identity
	if json.Unmarshal(b, &id) != nil {
		return
	}
	if a.secret == "" {
		a.secret = id.Secret
	}
	if a.nodeID == "" || a.nodeID == "node-local" {
		if id.NodeID != "" {
			a.nodeID = id.NodeID
			a.cfg.Cluster.NodeID = id.NodeID
		}
	}
}

func (a *Agent) saveIdentity() {
	if a.cfg.Cluster.IdentityFile == "" {
		return
	}
	b, _ := json.MarshalIndent(identity{NodeID: a.nodeID, Secret: a.secret, Master: a.currentMaster()}, "", "  ")
	writeFile(a.cfg.Cluster.IdentityFile, b)
}

func (a *Agent) currentMaster() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cur < len(a.masters) {
		return a.masters[a.cur]
	}
	return ""
}

// Secret 返回当前节点密钥。
func (a *Agent) Secret() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.secret
}

// Masters 返回主地址表。
func (a *Agent) Masters() []string { return a.masters }

// Enroll 使用加入令牌领取密钥；成功后写入身份文件。
func (a *Agent) Enroll(ctx context.Context) error {
	a.mu.Lock()
	token := a.token
	masters := append([]string{}, a.masters...)
	a.mu.Unlock()
	if token == "" || len(masters) == 0 {
		return nil
	}

	var lastErr error
	for i := 0; i < len(masters); i++ {
		payload := RegisterReq{
			NodeID: a.nodeID, Name: a.cfg.Cluster.NodeName, Addr: a.addr,
			Role: a.cfg.Cluster.Role, Weight: a.cfg.Cluster.Weight, Token: token,
		}
		body, _ := json.Marshal(payload)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(masters[i], "/internal/enroll"), bytes.NewReader(body))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := a.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		resp.Body.Close()
		var er EnrollResp
		if json.Unmarshal(b, &er) != nil || !er.OK {
			if er.Err != "" {
				lastErr = errors.New(er.Err)
			} else {
				lastErr = fmt.Errorf("enroll 失败: %d", resp.StatusCode)
			}
			continue
		}
		a.mu.Lock()
		a.secret = er.Secret
		if er.NodeID != "" {
			a.nodeID = er.NodeID
			a.cfg.Cluster.NodeID = er.NodeID
		}
		a.cur = i
		a.mu.Unlock()
		a.saveIdentity()
		return nil
	}
	if lastErr == nil {
		lastErr = errors.New("所有主节点地址均不可达")
	}
	return lastErr
}

func (a *Agent) Register(ctx context.Context, docCount int64, cfgStr string) error {
	return a.post(ctx, "/internal/register", RegisterReq{
		NodeID: a.nodeID, Name: a.cfg.Cluster.NodeName, Addr: a.addr,
		Role: a.cfg.Cluster.Role, Weight: a.cfg.Cluster.Weight, DocCount: docCount, Config: cfgStr,
		Version: a.cfg.Cluster.Version,
	})
}

func (a *Agent) Heartbeat(ctx context.Context, docCount int64, cfgStr string) error {
	return a.post(ctx, "/internal/heartbeat", RegisterReq{
		NodeID: a.nodeID, DocCount: docCount, Config: cfgStr, Version: a.cfg.Cluster.Version,
	})
}

// ResetPreference 回到地址表首位，用于主入口恢复后切回去。
func (a *Agent) ResetPreference() {
	a.mu.Lock()
	a.cur = 0
	a.mu.Unlock()
}

func (a *Agent) post(ctx context.Context, path string, payload interface{}) error {
	a.mu.Lock()
	n := len(a.masters)
	start, secret, nodeID := a.cur, a.secret, a.nodeID
	a.mu.Unlock()
	if n == 0 {
		return errors.New("未配置主节点地址")
	}
	if secret == "" {
		return errors.New("节点尚未领取密钥，请使用 -token 加入")
	}

	body, _ := json.Marshal(payload)
	var lastErr error
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if err := a.postTo(ctx, a.masters[idx], path, body, secret, nodeID); err != nil {
			lastErr = err
			continue
		}
		a.mu.Lock()
		changed := a.cur != idx
		old := a.cur
		a.cur = idx
		a.mu.Unlock()
		if changed {
			log.Printf("主节点切换到 %s（原 %s 不可用）", a.masters[idx], a.masters[old])
		}
		return nil
	}
	return lastErr
}

func (a *Agent) postTo(ctx context.Context, base, path string, body []byte, secret, nodeID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(base, path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	SignRequest(req, secret, nodeID, body)
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s 返回 %d: %s", base, resp.StatusCode, firstLine(string(b), 80))
	}
	return nil
}

func firstLine(s string, n int) string {
	rs := []rune(s)
	if len(rs) <= n {
		return s
	}
	return string(rs[:n])
}

func hostPart(raw string) string {
	s := raw
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if i := strings.IndexByte(s, '/'); i >= 0 {
		s = s[:i]
	}
	if i := strings.LastIndexByte(s, ':'); i > 0 {
		s = s[:i]
	}
	return s
}

func readFile(path string) ([]byte, error) { return os.ReadFile(path) }

func writeFile(path string, b []byte) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	os.WriteFile(path, b, 0o600)
}
