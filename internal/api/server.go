package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"queryza/frontend"
	"queryza/internal/auth"
	"queryza/internal/cluster"
	"queryza/internal/config"
	"queryza/internal/crawler"
	"queryza/internal/index"
	"queryza/internal/search"
	"queryza/internal/store"
)

type Server struct {
	cfg      *config.Config
	db       *store.DB
	cache    *store.Cache
	crawler  *crawler.Crawler
	indexer  *index.Indexer
	searcher *search.Searcher
	master   *cluster.Master
	keys     *auth.Manager
	admin    *auth.AdminAuth
	agent    *cluster.Agent

	startAt time.Time

	crawlMu     sync.Mutex
	crawlCancel context.CancelFunc
	crawlCtx    context.Context
}

func NewServer(cfg *config.Config, db *store.DB, cache *store.Cache, cw *crawler.Crawler,
	ix *index.Indexer, sr *search.Searcher, master *cluster.Master,
	keys *auth.Manager, admin *auth.AdminAuth, agent *cluster.Agent) *Server {
	return &Server{
		cfg: cfg, db: db, cache: cache, crawler: cw, indexer: ix, searcher: sr,
		master: master, keys: keys, admin: admin, agent: agent, startAt: time.Now(),
	}
}

// SetAgent 绑定子节点 Agent，供后台显示连接状态。
func (s *Server) SetAgent(a *cluster.Agent) { s.agent = a }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/internal/enroll", s.handleEnroll)
	mux.HandleFunc("/internal/crawl", s.handleInternalCrawl)
	mux.HandleFunc("/internal/register", s.handleRegister)
	mux.HandleFunc("/internal/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("/internal/search", s.handleInternalSearch)
	mux.HandleFunc("/internal/assign", s.handleAssign)
	mux.HandleFunc("/internal/ping", s.handlePing)
	mux.HandleFunc("/internal/docs", s.handleInternalDocs)

	// 子节点只提供内部接口：不带管理界面，也不对外暴露检索 API
	if s.cfg.Cluster.Role == "node" {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			writeErr(w, http.StatusNotFound, "node 仅提供内部接口")
		})
		return s.withCommon(mux)
	}

	mux.HandleFunc("/api/v1/", s.routeAPI)

	sub, _ := fs.Sub(frontend.FS, "admin")
	mux.Handle("/assets/", http.FileServer(http.FS(sub)))
	mux.HandleFunc(s.cfg.Server.AdminPath, s.handleIndex)
	mux.HandleFunc(s.cfg.Server.AdminPath+"/", s.handleIndex)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeErr(w, http.StatusNotFound, "not found")
	})
	return s.withCommon(mux)
}

func (s *Server) withCommon(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("X-Powered-By", "Queryza")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	b, err := frontend.FS.ReadFile("admin/index.html")
	if err != nil {
		writeErr(w, 500, "admin ui 未构建")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(b)
}

// ---------- 内部接口 ----------

// requireInternal 读取请求体并校验节点签名，返回节点 ID。
// 密钥为节点各自的密钥：主节点按节点 ID 查库，子节点用自身密钥。
func (s *Server) requireInternal(w http.ResponseWriter, r *http.Request) (string, []byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		writeErr(w, 400, "读取请求体失败")
		return "", nil, false
	}
	r.Body.Close()
	if !s.cfg.Cluster.RequireAuth {
		return r.Header.Get(cluster.HdrNode), body, true
	}
	nodeID, err := cluster.VerifyRequest(r, body, func(id string) (string, error) {
		if id == s.cfg.Cluster.NodeID {
			return s.cfg.Cluster.Secret, nil
		}
		return s.master.NodeSecret(id)
	})
	if err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return "", nil, false
	}
	return nodeID, body, true
}

func clientIP(r *http.Request) string {
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		if i := strings.IndexByte(v, ','); i > 0 {
			return strings.TrimSpace(v[:i])
		}
		return strings.TrimSpace(v)
	}
	if v := r.Header.Get("X-Real-IP"); v != "" {
		return v
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) handleEnroll(w http.ResponseWriter, r *http.Request) {
	var req cluster.RegisterReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 200, cluster.EnrollResp{OK: false, Err: "请求格式错误"})
		return
	}
	nodeID, secret, err := s.master.Enroll(req.Token)
	if err != nil {
		writeJSON(w, 200, cluster.EnrollResp{OK: false, Err: err.Error()})
		return
	}
	writeJSON(w, 200, cluster.EnrollResp{
		OK: true, NodeID: nodeID, Secret: secret, MasterID: s.cfg.Cluster.NodeID,
	})
}

// handleInternalCrawl 由主节点下发抓取开关。
func (s *Server) handleInternalCrawl(w http.ResponseWriter, r *http.Request) {
	_, body, ok := s.requireInternal(w, r)
	if !ok {
		return
	}
	var req struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, 400, "bad request")
		return
	}
	switch req.Action {
	case "start":
		if err := s.startCrawl(r.Context()); err != nil {
			writeErr(w, 500, err.Error())
			return
		}
	case "stop":
		s.stopCrawl()
	default:
		writeErr(w, 400, "未知动作")
		return
	}
	writeJSON(w, 200, map[string]interface{}{"ok": true, "running": s.crawlRunning()})
}

func (s *Server) handleInternalDocs(w http.ResponseWriter, r *http.Request) {
	_, body, ok := s.requireInternal(w, r)
	if !ok {
		return
	}
	var req cluster.InternalDocsReq
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, 200, cluster.InternalDocsResp{OK: false, Err: "bad request"})
		return
	}
	q := store.DocQuery{Page: req.Page, Size: req.Size, Q: req.Q, Domain: req.Domain, Shard: -1}
	if req.Shard != nil {
		q.Shard = *req.Shard
	}
	docs, total, err := s.db.QueryDocs(r.Context(), q)
	if err != nil {
		writeJSON(w, 200, cluster.InternalDocsResp{OK: false, Err: err.Error()})
		return
	}
	st := s.db.Stats(r.Context())
	writeJSON(w, 200, cluster.InternalDocsResp{
		OK: true, Docs: docs, Total: total, Page: q.Page, Size: q.Size,
		Stats: &st, TopDomains: s.db.TopDomains(r.Context(), 12),
	})
}

func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"ok": true, "node_id": s.cfg.Cluster.NodeID, "role": s.cfg.Cluster.Role,
	})
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	nodeID, body, ok := s.requireInternal(w, r)
	if !ok {
		return
	}
	var req cluster.RegisterReq
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, 400, "bad request")
		return
	}
	if req.NodeID == "" {
		req.NodeID = nodeID
	}
	if req.NodeID != nodeID {
		writeErr(w, 403, "节点标识与签名不一致")
		return
	}
	req.IP = clientIP(r)
	if err := s.master.Register(req); err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return
	}
	writeJSON(w, 200, map[string]interface{}{"ok": true, "master": s.cfg.Cluster.NodeID})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	nodeID, body, ok := s.requireInternal(w, r)
	if !ok {
		return
	}
	var req cluster.RegisterReq
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, 400, "bad request")
		return
	}
	id := req.NodeID
	if id == "" {
		id = nodeID
	}
	if err := s.master.Heartbeat(id, req.DocCount, req.Config, req.Version, clientIP(r)); err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return
	}
	writeJSON(w, 200, map[string]interface{}{"ok": true})
}

func (s *Server) handleInternalSearch(w http.ResponseWriter, r *http.Request) {
	nodeID, body, ok := s.requireInternal(w, r)
	if !ok {
		return
	}
	var req cluster.InternalSearchReq
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, 200, cluster.InternalSearchResp{OK: false, Err: "bad request"})
		return
	}
	// 只在本节点负责的域名内召回，主节点按副本归并，避免同一文档被多节点重复返回
	_ = nodeID
	allow := s.ownedDomains()
	if len(req.Domains) > 0 {
		// 主节点指定代查域名（副本接管故障节点）
		allow = make(map[string]bool, len(req.Domains))
		for _, d := range req.Domains {
			allow[d] = true
		}
	}
	res, err := s.searcher.SearchLocalWithin(r.Context(), req.Query, req.TopK, allow)
	if err != nil {
		writeJSON(w, 200, cluster.InternalSearchResp{OK: false, Err: err.Error()})
		return
	}
	writeJSON(w, 200, cluster.InternalSearchResp{OK: true, Result: res})
}

func (s *Server) ownedDomains() map[string]bool {
	list := s.master.DomainsOf(s.cfg.Cluster.NodeID)
	if len(list) == 0 {
		return nil
	}
	out := make(map[string]bool, len(list))
	for _, d := range list {
		out[d] = true
	}
	return out
}

func (s *Server) handleAssign(w http.ResponseWriter, r *http.Request) {
	domain := r.URL.Query().Get("domain")
	node := s.master.AssignDomain(domain)
	writeJSON(w, 200, map[string]interface{}{"domain": domain, "node_id": node})
}

// ---------- API 路由 ----------

func (s *Server) routeAPI(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimSuffix(r.URL.Path, "/")
	switch {
	case p == "/api/v1/auth/login" && r.Method == http.MethodPost:
		s.handleLogin(w, r)
	case p == "/api/v1/search":
		s.handlePublicSearch(w, r)
	case p == "/api/v1/suggest":
		s.handleSuggest(w, r)
	case strings.HasPrefix(p, "/api/v1/admin/"):
		if !s.checkAdmin(r) {
			writeErr(w, http.StatusUnauthorized, "未登录或登录已过期")
			return
		}
		s.routeAdmin(w, r, strings.TrimPrefix(p, "/api/v1/admin/"))
	default:
		writeErr(w, 404, "not found")
	}
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "参数错误")
		return
	}
	tok, ok := s.admin.Login(req.Username, req.Password)
	if !ok {
		writeErr(w, 401, "用户名或密码错误")
		return
	}
	writeJSON(w, 200, map[string]interface{}{"token": tok, "expires_in": 86400})
}

func (s *Server) checkAdmin(r *http.Request) bool {
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, "Bearer ") {
		return s.admin.Verify(strings.TrimPrefix(h, "Bearer "))
	}
	return false
}

func (s *Server) extractAPIKey(r *http.Request) string {
	if k := r.Header.Get("X-API-Key"); k != "" {
		return k
	}
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, "Bearer qz_") {
		return strings.TrimPrefix(h, "Bearer ")
	}
	return ""
}

func (s *Server) handlePublicSearch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query  string `json:"query"`
		Q      string `json:"q"`
		TopK   int    `json:"top_k"`
		Size   int    `json:"size"`
		Page   int    `json:"page"`
		Offset int    `json:"offset"`
	}
	q := r.URL.Query().Get("q")
	req.Size = atoiDefault(r.URL.Query().Get("size"), 0)
	if req.Size == 0 {
		req.Size = atoiDefault(r.URL.Query().Get("top_k"), 0)
	}
	req.Page = atoiDefault(r.URL.Query().Get("page"), 1)
	req.Offset = atoiDefault(r.URL.Query().Get("offset"), -1)

	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
			if req.Query != "" {
				q = req.Query
			} else if req.Q != "" {
				q = req.Q
			}
		}
	}
	key, err := s.keys.Verify(r.Context(), s.extractAPIKey(r))
	if err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}

	size := req.Size
	if size <= 0 {
		size = s.cfg.Search.TopK
	}
	if size > 100 {
		size = 100
	}
	if key.ResultMax > 0 && size > key.ResultMax {
		size = key.ResultMax
	}
	page := req.Page
	if page < 1 {
		page = 1
	}
	offset := req.Offset
	if offset < 0 {
		offset = (page - 1) * size
	} else {
		page = offset/size + 1
	}

	res, err := s.master.Search(r.Context(), q, size, offset)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	res.Paginate(page, size)
	s.db.ExecContext(r.Context(), `INSERT INTO query_logs(query,results,latency_ms,key_id,node_id) VALUES(?,?,?,?,?)`,
		q, len(res.Hits), res.TookMS, key.ID, s.cfg.Cluster.NodeID)
	writeJSON(w, 200, res)
}

// handleSuggest 前缀补全，供搜索框联想；与检索共用 API 密钥。
func (s *Server) handleSuggest(w http.ResponseWriter, r *http.Request) {
	if _, err := s.keys.Verify(r.Context(), s.extractAPIKey(r)); err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	writeJSON(w, 200, map[string]interface{}{
		"query": r.URL.Query().Get("q"),
		"items": s.suggest(r.Context(), r.URL.Query().Get("q")),
	})
}

// suggest 联想词：优先历史真实查询（完整短语），再用词项前缀兜底。
// bigram 索引里没有完整中文词，只靠 terms 前缀补全效果很差。
func (s *Server) suggest(ctx context.Context, prefix string) []string {
	prefix = strings.TrimSpace(prefix)
	items := []string{}
	if prefix == "" {
		return items
	}
	seen := map[string]bool{}
	rows, err := s.db.QueryContext(ctx, `SELECT query FROM query_logs
		WHERE query LIKE ? AND CHAR_LENGTH(query) <= 40
		GROUP BY query ORDER BY COUNT(*) DESC LIMIT 10`, prefix+"%")
	if err == nil {
		for rows.Next() {
			var t string
			if rows.Scan(&t) == nil && !seen[t] {
				seen[t] = true
				items = append(items, t)
			}
		}
		rows.Close()
	}
	if len(items) < 8 {
		rows, err := s.db.QueryContext(ctx, `SELECT term FROM terms
			WHERE term LIKE ? ORDER BY df DESC LIMIT 10`, prefix+"%")
		if err == nil {
			for rows.Next() {
				var t string
				if rows.Scan(&t) == nil && !seen[t] {
					seen[t] = true
					items = append(items, t)
				}
			}
			rows.Close()
		}
	}
	if len(items) > 10 {
		items = items[:10]
	}
	return items
}

func (s *Server) startCrawl(ctx context.Context) error {
	s.crawlMu.Lock()
	defer s.crawlMu.Unlock()
	if s.crawlCancel != nil {
		return nil
	}
	if _, err := s.crawler.LoadSeeds(ctx); err != nil {
		return err
	}
	cctx, cancel := context.WithCancel(context.Background())
	s.crawlCtx = cctx
	s.crawlCancel = cancel
	go func() {
		s.crawler.Run(cctx)
		s.crawlMu.Lock()
		s.crawlCancel = nil
		s.crawlMu.Unlock()
	}()
	return nil
}

func (s *Server) stopCrawl() {
	s.crawlMu.Lock()
	defer s.crawlMu.Unlock()
	if s.crawlCancel != nil {
		s.crawlCancel()
		s.crawlCancel = nil
	}
}

func (s *Server) crawlRunning() bool {
	s.crawlMu.Lock()
	defer s.crawlMu.Unlock()
	return s.crawlCancel != nil
}

// ---------- helpers ----------

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]interface{}{"error": msg, "status": status})
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	if h > 0 {
		return fmt.Sprintf("%dh%dm", h, m)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
