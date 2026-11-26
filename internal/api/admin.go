package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"queryza/internal/config"
	"queryza/internal/crawler"
	"queryza/internal/store"
)

func (s *Server) routeAdmin(w http.ResponseWriter, r *http.Request, sub string) {
	switch sub {
	case "overview":
		s.aOverview(w, r)
	case "nodes":
		s.aNodes(w, r)
	case "nodes/create":
		s.aNodeCreate(w, r)
	case "nodes/delete":
		s.aNodeDelete(w, r)
	case "nodes/reset":
		s.aNodeReset(w, r)
	case "nodes/weight":
		s.aNodeWeight(w, r)
	case "nodes/config":
		s.aNodeConfig(w, r)
	case "rules":
		s.aRules(w, r)
	case "seeds":
		s.aSeeds(w, r)
	case "crawl/stats":
		s.aCrawlStats(w, r)
	case "crawl/start":
		s.aCrawlStart(w, r)
	case "crawl/stop":
		s.aCrawlStop(w, r)
	case "crawl/enqueue":
		s.aCrawlEnqueue(w, r)
	case "crawl/logs":
		s.aCrawlLogs(w, r)
	case "docs":
		s.aDocs(w, r)
	case "keys":
		s.aKeys(w, r)
	case "keys/toggle":
		s.aKeysToggle(w, r)
	case "keys/delete":
		s.aKeysDelete(w, r)
	case "config":
		s.aConfig(w, r)
	case "assignments":
		s.aAssignments(w, r)
	case "hot":
		s.aHot(w, r)
	case "search":
		s.aSearch(w, r)
	case "query-logs":
		s.aQueryLogs(w, r)
	default:
		writeErr(w, 404, "未知接口: "+sub)
	}
}

func (s *Server) aOverview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var docs, domains, todayFetched, todayQueries int64
	s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents`).Scan(&docs)
	s.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT domain) FROM documents`).Scan(&domains)
	s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM crawl_logs WHERE created_at >= CURDATE()`).Scan(&todayFetched)
	s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM query_logs WHERE created_at >= CURDATE()`).Scan(&todayQueries)

	queue := s.crawler.Frontier().Stats(ctx)
	nodes := s.master.ListNodes()
	online := 0
	for _, n := range nodes {
		if n.Status == "online" {
			online++
		}
	}
	hot := s.hotQueries(ctx, 8)

	writeJSON(w, 200, map[string]interface{}{
		"docs":          docs,
		"domains":       domains,
		"queue":         queue,
		"nodes_total":   len(nodes),
		"nodes_online":  online,
		"today_fetched": todayFetched,
		"today_queries": todayQueries,
		"crawl":         s.crawler.Stats(),
		"crawl_running": s.crawlRunning(),
		"hot_queries":   hot,
		"role":          s.cfg.Cluster.Role,
		"node_id":       s.cfg.Cluster.NodeID,
		"uptime":        fmtDuration(time.Since(s.startAt)),
		"search_mode":   searchMode(s.cfg),
	})
}

func searchMode(c *config.Config) string {
	if c.Search.Hybrid && c.Embedding.Enabled {
		return "hybrid"
	}
	return "keyword"
}

func (s *Server) aNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"nodes":          s.master.ListNodes(),
		"self_id":        s.cfg.Cluster.NodeID,
		"replica_factor": s.cfg.Cluster.ReplicaFactor,
		"require_auth":   s.cfg.Cluster.RequireAuth,
		"join_bases":     s.joinBases(),
	})
}

// joinBases 节点加入时使用的主节点地址（优先用地址表首项）
func (s *Server) joinBases() []string {
	if len(s.cfg.Cluster.MasterAddrs) > 0 {
		return s.cfg.Cluster.MasterAddrs
	}
	if s.cfg.Cluster.AdvertiseAddr != "" {
		return []string{s.cfg.Cluster.AdvertiseAddr}
	}
	return nil
}

func (s *Server) aNodeCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		NodeID string  `json:"node_id"`
		Name   string  `json:"name"`
		Addr   string  `json:"addr"`
		Weight float64 `json:"weight"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "参数错误")
		return
	}
	nodeID, token, err := s.master.CreateNode(body.NodeID, body.Name, body.Addr, body.Weight)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	base := ""
	if b := s.joinBases(); len(b) > 0 {
		base = b[0]
	}
	cmd := fmt.Sprintf("./queryza -config configs/config.yaml -role node -node-id %s -master %s -token %s",
		nodeID, base, token)
	writeJSON(w, 200, map[string]interface{}{
		"ok": true, "node_id": nodeID, "token": token, "command": cmd,
		"expires_in": 86400,
	})
}

func (s *Server) aNodeDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		NodeID string `json:"node_id"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if err := s.master.DeleteNode(body.NodeID); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	writeJSON(w, 200, map[string]interface{}{"ok": true})
}

func (s *Server) aNodeReset(w http.ResponseWriter, r *http.Request) {
	var body struct {
		NodeID string `json:"node_id"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	token, err := s.master.ResetNodeSecret(body.NodeID)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	base := ""
	if b := s.joinBases(); len(b) > 0 {
		base = b[0]
	}
	writeJSON(w, 200, map[string]interface{}{
		"ok": true, "token": token,
		"command": fmt.Sprintf("./queryza -config configs/config.yaml -role node -node-id %s -master %s -token %s",
			body.NodeID, base, token),
	})
}

func (s *Server) aNodeWeight(w http.ResponseWriter, r *http.Request) {
	var body struct {
		NodeID string  `json:"node_id"`
		Weight float64 `json:"weight"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if err := s.master.SetNodeWeight(body.NodeID, body.Weight); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]interface{}{"ok": true})
}

// aNodeConfig 读取 / 保存单个节点的独立配置（并发、同域间隔、代理组、请求头等）。
func (s *Server) aNodeConfig(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("node_id")
	if r.Method == http.MethodGet {
		ov := s.nodeOverrides()
		if nodeID == "" {
			writeJSON(w, 200, map[string]interface{}{"overrides": ov})
			return
		}
		writeJSON(w, 200, map[string]interface{}{
			"node_id":  nodeID,
			"override": ov[nodeID],
			"defaults": map[string]interface{}{
				"concurrency":            s.cfg.Crawler.Concurrency,
				"per_domain_interval_ms": s.cfg.Crawler.PerDomainIntervalMS,
				"max_pages_per_run":      s.cfg.Crawler.MaxPagesPerRun,
				"max_db_docs":            s.cfg.Limits.MaxDBDocsPerNode,
				"max_redis_docs":         s.cfg.Limits.MaxRedisDocs,
				"mode":                   s.cfg.Crawler.Mode,
				"proxy_group":            s.cfg.Crawler.ProxyGroup,
			},
			"hot_fields": []string{"concurrency", "per_domain_interval_ms", "max_pages_per_run", "weight"},
			"self_id":    s.cfg.Cluster.NodeID,
		})
		return
	}

	var body struct {
		NodeID string `json:"node_id"`
		config.NodeOverride
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.NodeID == "" {
		writeErr(w, 400, "参数错误")
		return
	}
	all := s.nodeOverrides()
	all[body.NodeID] = body.NodeOverride
	b, _ := json.Marshal(all)
	if err := s.db.SetConfig("node_override", string(b)); err != nil {
		writeErr(w, 500, err.Error())
		return
	}

	// 本节点立即热生效；其它节点在 20 秒内的配置轮询中自动应用
	applied := false
	if body.NodeID == s.cfg.Cluster.NodeID {
		s.applyHotConfig(body.NodeOverride)
		applied = true
	}
	writeJSON(w, 200, map[string]interface{}{
		"ok": true, "applied_now": applied,
		"note": "并发、同域间隔、单次上限、权重立即生效；模式/代理组/请求头在节点下一轮轮询后生效",
	})
}

func (s *Server) applyHotConfig(o config.NodeOverride) {
	if o.Concurrency != nil {
		s.crawler.SetConcurrency(*o.Concurrency)
	}
	if o.PerDomainIntervalMS != nil {
		s.crawler.SetPerDomainIntervalMS(*o.PerDomainIntervalMS)
	}
	if o.MaxPagesPerRun != nil {
		s.crawler.SetMaxPages(*o.MaxPagesPerRun)
	}
	if o.Weight != nil && *o.Weight > 0 {
		s.master.SetNodeWeight(s.cfg.Cluster.NodeID, *o.Weight)
	}
}

func (s *Server) nodeOverrides() map[string]config.NodeOverride {
	out := map[string]config.NodeOverride{}
	raw := s.db.GetConfig("node_override", "")
	if raw != "" {
		_ = json.Unmarshal([]byte(raw), &out)
	}
	return out
}

func (s *Server) aRules(w http.ResponseWriter, r *http.Request) {
	list := crawler.RuleList()
	writeJSON(w, 200, map[string]interface{}{"rules": list, "total": len(list)})
}

func (s *Server) aSeeds(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method == http.MethodGet {
		rows, err := s.db.QueryContext(ctx, `SELECT id,url,domain,name,category,weight,enabled,depth
			FROM seeds ORDER BY weight DESC, id ASC`)
		if err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		defer rows.Close()
		var out []store.Seed
		for rows.Next() {
			var sd store.Seed
			if rows.Scan(&sd.ID, &sd.URL, &sd.Domain, &sd.Name, &sd.Category, &sd.Weight, &sd.Enabled, &sd.Depth) == nil {
				out = append(out, sd)
			}
		}
		writeJSON(w, 200, map[string]interface{}{"seeds": out, "total": len(out)})
		return
	}
	var body struct {
		URL      string  `json:"url"`
		Name     string  `json:"name"`
		Category string  `json:"category"`
		Weight   float64 `json:"weight"`
		Depth    int     `json:"depth"`
		Enqueue  bool    `json:"enqueue"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.URL == "" {
		writeErr(w, 400, "参数错误")
		return
	}
	if body.Weight <= 0 {
		body.Weight = 1
	}
	if body.Depth <= 0 {
		body.Depth = s.cfg.Crawler.MaxDepth
	}
	host := hostOfURL(body.URL)
	_, err := s.db.ExecContext(ctx, `INSERT INTO seeds(url,url_hash,domain,name,category,weight,enabled,depth)
		VALUES(?,?,?,?,?,?,1,?) ON DUPLICATE KEY UPDATE name=VALUES(name),category=VALUES(category),weight=VALUES(weight)`,
		body.URL, hashOf(body.URL), host, body.Name, body.Category, body.Weight, body.Depth)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if body.Enqueue {
		s.crawler.EnqueueURLs(ctx, []string{body.URL}, 0, int(body.Weight*100))
	}
	writeJSON(w, 200, map[string]interface{}{"ok": true})
}

func (s *Server) aCrawlStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"stats":   s.crawler.Stats(),
		"queue":   s.crawler.Frontier().Stats(r.Context()),
		"running": s.crawlRunning(),
	})
}

func (s *Server) aCrawlStart(w http.ResponseWriter, r *http.Request) {
	// 队列被清空后仍能一键恢复：种子表入库并补入队（幂等）
	if n, err := s.crawler.LoadSeeds(r.Context()); err == nil && n > 0 {
		_ = n
	}
	if err := s.startCrawl(r.Context()); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	// 节点不带界面，抓取由主节点统一下发
	failed := s.master.BroadcastCrawl(r.Context(), "start")
	writeJSON(w, 200, map[string]interface{}{"ok": true, "running": true, "failed_nodes": failed})
}

func (s *Server) aCrawlStop(w http.ResponseWriter, r *http.Request) {
	s.stopCrawl()
	failed := s.master.BroadcastCrawl(r.Context(), "stop")
	writeJSON(w, 200, map[string]interface{}{"ok": true, "running": false, "failed_nodes": failed})
}

func (s *Server) aCrawlEnqueue(w http.ResponseWriter, r *http.Request) {
	var body struct {
		URLs     []string `json:"urls"`
		URL      string   `json:"url"`
		Depth    int      `json:"depth"`
		Priority int      `json:"priority"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "参数错误")
		return
	}
	if body.URL != "" {
		body.URLs = append(body.URLs, body.URL)
	}
	n, err := s.crawler.EnqueueURLs(r.Context(), body.URLs, body.Depth, body.Priority)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]interface{}{"ok": true, "added": n})
}

func (s *Server) aCrawlLogs(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `SELECT id,url,domain,status,http_code,bytes,error,duration_ms,node_id,COALESCE(rule_id,''),created_at
		FROM crawl_logs ORDER BY id DESC LIMIT 120`)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	defer rows.Close()
	var out []store.CrawlLog
	for rows.Next() {
		var l store.CrawlLog
		if rows.Scan(&l.ID, &l.URL, &l.Domain, &l.Status, &l.HTTPCode, &l.Bytes, &l.Error, &l.DurationMS, &l.NodeID, &l.RuleID, &l.CreatedAt) == nil {
			out = append(out, l)
		}
	}
	writeJSON(w, 200, map[string]interface{}{"logs": out})
}

func (s *Server) aDocs(w http.ResponseWriter, r *http.Request) {
	page := atoiDefault(r.URL.Query().Get("page"), 1)
	size := atoiDefault(r.URL.Query().Get("size"), 20)
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 100 {
		size = 20
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	where := ""
	args := []interface{}{}
	if q != "" {
		where = " WHERE title LIKE ? OR domain LIKE ?"
		args = append(args, "%"+q+"%", "%"+q+"%")
	}
	var total int64
	s.db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM documents`+where, args...).Scan(&total)
	args = append(args, size, (page-1)*size)
	rows, err := s.db.QueryContext(r.Context(), `SELECT id,url,title,domain,content_len,http_status,fetched_at,
		site_weight,dynamic_weight,trust_score,spam_score,index_status
		FROM documents`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`, args...)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	defer rows.Close()
	var out []map[string]interface{}
	for rows.Next() {
		var id, clen int64
		var url, title, domain string
		var status, ist int
		var fetched time.Time
		var site, dyn, trust, spam float64
		if rows.Scan(&id, &url, &title, &domain, &clen, &status, &fetched, &site, &dyn, &trust, &spam, &ist) == nil {
			out = append(out, map[string]interface{}{
				"id": id, "url": url, "title": title, "domain": domain, "content_len": clen,
				"http_status": status, "fetched_at": fetched.Format("2006-01-02 15:04"),
				"site_weight": site, "dynamic_weight": dyn, "trust_score": trust, "spam_score": spam,
				"index_status": ist,
			})
		}
	}
	writeJSON(w, 200, map[string]interface{}{"docs": out, "total": total, "page": page, "size": size})
}

func (s *Server) aKeys(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method == http.MethodGet {
		list, _ := s.keys.List(ctx)
		writeJSON(w, 200, map[string]interface{}{"keys": list})
		return
	}
	var body struct {
		Name      string `json:"name"`
		QPS       int    `json:"qps_limit"`
		Daily     int    `json:"daily_quota"`
		ResultMax int    `json:"result_max"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	raw, k, err := s.keys.Create(ctx, body.Name, body.QPS, body.Daily, body.ResultMax)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]interface{}{"ok": true, "key": raw, "info": k})
}

func (s *Server) aKeysToggle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID      int64 `json:"id"`
		Enabled bool  `json:"enabled"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	s.keys.SetEnabled(r.Context(), body.ID, body.Enabled)
	writeJSON(w, 200, map[string]interface{}{"ok": true})
}

func (s *Server) aKeysDelete(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID int64 `json:"id"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	s.keys.Delete(r.Context(), body.ID)
	writeJSON(w, 200, map[string]interface{}{"ok": true})
}

func (s *Server) aConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, 200, s.configSnapshot())
		return
	}
	var rc config.RuntimeConfig
	if err := json.NewDecoder(r.Body).Decode(&rc); err != nil {
		writeErr(w, 400, "参数错误")
		return
	}
	b, _ := json.Marshal(rc)
	if err := s.cfg.ApplyRuntimeJSON(string(b)); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	s.db.SetConfig("runtime_config", s.cfg.RuntimeJSON())
	writeJSON(w, 200, map[string]interface{}{"ok": true, "note": "已保存；搜索权重等部分配置在重启后完全生效"})
}

func (s *Server) configSnapshot() map[string]interface{} {
	return map[string]interface{}{
		"search": s.cfg.Search,
		"limits": s.cfg.Limits,
		"crawler": map[string]interface{}{
			"mode": s.cfg.Crawler.Mode, "enabled": s.cfg.Crawler.Enabled,
			"proxy_group": s.cfg.Crawler.ProxyGroup, "default_headers": s.cfg.Crawler.DefaultHeaders,
			"seed_file": s.cfg.Crawler.SeedFile, "respect_robots": s.cfg.Crawler.RespectRobots,
			"max_depth": s.cfg.Crawler.MaxDepth, "concurrency": s.cfg.Crawler.Concurrency,
			"max_pages_per_run": s.cfg.Crawler.MaxPagesPerRun,
			"allow_domains":     s.cfg.Crawler.AllowDomains, "block_domains": s.cfg.Crawler.BlockDomains,
		},
		"embedding": map[string]interface{}{
			"enabled": s.cfg.Embedding.Enabled, "base_url": s.cfg.Embedding.BaseURL,
			"model": s.cfg.Embedding.Model, "dim": s.cfg.Embedding.Dim,
			"api_key_set": s.cfg.Embedding.APIKey != "",
		},
		"cluster": map[string]interface{}{
			"role": s.cfg.Cluster.Role, "node_id": s.cfg.Cluster.NodeID, "node_name": s.cfg.Cluster.NodeName,
			"weight": s.cfg.Cluster.Weight, "fanout_mode": s.cfg.Cluster.FanoutMode,
		},
	}
}

func (s *Server) aAssignments(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{"assignments": s.master.Assignments()})
}

func (s *Server) aHot(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{"hot": s.hotQueries(r.Context(), 20)})
}

func (s *Server) hotQueries(ctx context.Context, n int) []map[string]interface{} {
	if s.cache == nil {
		return nil
	}
	key := "qz:hot:" + time.Now().Format("2006-01-02")
	res, err := s.cache.ZRevRangeWithScores(ctx, key, 0, int64(n-1)).Result()
	if err != nil {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(res))
	for _, z := range res {
		out = append(out, map[string]interface{}{"query": z.Member, "count": int64(z.Score)})
	}
	return out
}

func (s *Server) aSearch(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Query string `json:"query"`
		TopK  int    `json:"top_k"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "参数错误")
		return
	}
	if body.TopK <= 0 {
		body.TopK = s.cfg.Search.TopK
	}
	res, err := s.master.Search(r.Context(), body.Query, body.TopK)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, res)
}

func (s *Server) aQueryLogs(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), `SELECT query,results,latency_ms,created_at
		FROM query_logs ORDER BY id DESC LIMIT 80`)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	defer rows.Close()
	var out []map[string]interface{}
	for rows.Next() {
		var q string
		var res, lat int
		var at time.Time
		if rows.Scan(&q, &res, &lat, &at) == nil {
			out = append(out, map[string]interface{}{"query": q, "results": res, "latency_ms": lat, "created_at": at.Format("01-02 15:04:05")})
		}
	}
	writeJSON(w, 200, map[string]interface{}{"logs": out})
}

func hostOfURL(u string) string {
	pu, err := url.Parse(u)
	if err != nil {
		return ""
	}
	return pu.Host
}

func hashOf(u string) string { return crawler.URLHash(u) }
