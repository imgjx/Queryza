package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
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

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/internal/register", s.handleRegister)
	mux.HandleFunc("/internal/heartbeat", s.handleHeartbeat)
	mux.HandleFunc("/internal/search", s.handleInternalSearch)
	mux.HandleFunc("/internal/assign", s.handleAssign)

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

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req cluster.RegisterReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "bad request")
		return
	}
	s.master.Register(req)
	writeJSON(w, 200, map[string]interface{}{"ok": true})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req cluster.RegisterReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "bad request")
		return
	}
	s.master.Heartbeat(req.NodeID, req.DocCount, req.Config)
	writeJSON(w, 200, map[string]interface{}{"ok": true})
}

func (s *Server) handleInternalSearch(w http.ResponseWriter, r *http.Request) {
	var req cluster.InternalSearchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 200, cluster.InternalSearchResp{OK: false, Err: "bad request"})
		return
	}
	res, err := s.searcher.SearchLocal(r.Context(), req.Query, req.TopK)
	if err != nil {
		writeJSON(w, 200, cluster.InternalSearchResp{OK: false, Err: err.Error()})
		return
	}
	writeJSON(w, 200, cluster.InternalSearchResp{OK: true, Result: res})
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
	q := r.URL.Query().Get("q")
	if r.Method == http.MethodPost {
		var body struct {
			Query string `json:"query"`
			Q     string `json:"q"`
			TopK  int    `json:"top_k"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if body.Query != "" {
			q = body.Query
		} else if body.Q != "" {
			q = body.Q
		}
	}
	key, err := s.keys.Verify(r.Context(), s.extractAPIKey(r))
	if err != nil {
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	topK := atoiDefault(r.URL.Query().Get("top_k"), s.cfg.Search.TopK)
	if key.ResultMax > 0 && topK > key.ResultMax {
		topK = key.ResultMax
	}
	res, err := s.master.Search(r.Context(), q, topK)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	s.db.ExecContext(r.Context(), `INSERT INTO query_logs(query,results,latency_ms,key_id,node_id) VALUES(?,?,?,?,?)`,
		q, len(res.Hits), res.TookMS, key.ID, s.cfg.Cluster.NodeID)
	writeJSON(w, 200, res)
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
