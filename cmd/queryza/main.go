package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"queryza/internal/api"
	"queryza/internal/auth"
	"queryza/internal/cluster"
	"queryza/internal/config"
	"queryza/internal/crawler"
	"queryza/internal/index"
	"queryza/internal/search"
	"queryza/internal/store"
)

// version 由构建时 -ldflags "-X main.version=..." 注入
var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "打印版本号并退出")
	cfgPath := flag.String("config", "configs/config.yaml", "配置文件")
	role := flag.String("role", "", "角色: standalone|master|node")
	nodeID := flag.String("node-id", "", "节点 ID")
	addr := flag.String("addr", "", "监听地址，如 :8080")
	adminPath := flag.String("admin-path", "", "管理后台路径")
	masterAddr := flag.String("master", "", "主节点地址（node 角色），多个用逗号分隔")
	advertise := flag.String("advertise", "", "本节点对外地址（用于注册）")
	token := flag.String("token", "", "加入令牌（首次加入集群）")
	secret := flag.String("secret", "", "节点通信密钥")
	replica := flag.Int("replica", 0, "每个域名的副本数（主节点）")
	flag.Parse()

	if *showVersion {
		println("queryza " + version)
		return
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	if *role != "" {
		cfg.Cluster.Role = *role
	}
	if *nodeID != "" {
		cfg.Cluster.NodeID = *nodeID
	} else if cfg.Cluster.Role == "node" && cfg.Cluster.NodeID == "node-local" {
		cfg.Cluster.NodeID = "node-" + randSuffix()
	}
	if *addr != "" {
		cfg.Server.Addr = *addr
	}
	if *adminPath != "" {
		cfg.Server.AdminPath = normalizePath(*adminPath)
	}
	if *masterAddr != "" {
		cfg.Cluster.MasterAddrs = splitList(*masterAddr)
		cfg.Cluster.MasterAddr = cfg.Cluster.MasterAddrs[0]
	}
	if *token != "" {
		cfg.Cluster.Token = *token
	}
	if *secret != "" {
		cfg.Cluster.Secret = *secret
	}
	if *replica > 0 {
		cfg.Cluster.ReplicaFactor = *replica
	}
	if *advertise != "" {
		cfg.Cluster.AdvertiseAddr = *advertise
	}
	if cfg.Cluster.AdvertiseAddr == "" {
		cfg.Cluster.AdvertiseAddr = "http://127.0.0.1" + cfg.Server.Addr
	}
	cfg.Cluster.Version = version
	cfg.ApplyNodeOverride(cfg.Cluster.NodeID)

	db, err := store.OpenDB(cfg.Database.DSN, cfg.Database.MaxOpenConns, cfg.Database.MaxIdleConns, cfg.Database.ConnMaxLifetime)
	if err != nil {
		log.Fatalf("数据库: %v", err)
	}
	if err := db.Migrate(); err != nil {
		log.Fatalf("初始化数据库: %v", err)
	}
	cache, err := store.OpenRedis(cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB, cfg.Redis.PoolSize)
	if err != nil {
		log.Printf("警告: Redis 不可用，将降级运行: %v", err)
		cache = nil
	}

	if rc := db.GetConfig("runtime_config", ""); rc != "" {
		cfg.ApplyRuntimeJSON(rc)
	}

	embedder := index.NewEmbedder(cfg.Embedding)
	indexer := index.NewIndexer(db, cache, cfg.Limits, embedder)
	searcher := search.New(db, cache, cfg.Search, embedder, cfg.Cluster.NodeID)
	// 内存倒排索引：后台全量加载，就绪后检索走内存
	memIdx := index.NewMemIndex(cfg.Limits.MaxMemDocs)
	indexer.SetMemIndex(memIdx)
	searcher.SetMemIndex(memIdx)
	go memIdx.Build(db)

	proxies := loadProxyGroup(db, cfg.Crawler.ProxyGroup)
	cr := crawler.New(cfg, db, cache, cfg.Cluster.NodeID, proxies, indexer)

	master := cluster.NewMaster(cfg, db, searcher)
	selfCfg, _ := json.Marshal(cfg.SelfConfig())
	master.Register(cluster.RegisterReq{
		NodeID: cfg.Cluster.NodeID, Name: cfg.Cluster.NodeName, Addr: cfg.Cluster.AdvertiseAddr,
		Role: cfg.Cluster.Role, Weight: cfg.Cluster.Weight, DocCount: 0, Config: string(selfCfg), Version: version,
	})
	// 域名按主副本归属：只有主副本负责抓取与入库
	cr.SetOwnership(master)

	// 后台「节点管理」里设置的独立配置：启动时全量应用，运行时每 20 秒轮询热更
	lastCfgFp := applyNodeOverride(db, cfg, cr)

	ctx, stopContext := context.WithCancel(context.Background())
	defer stopContext()
	go watchNodeOverride(ctx, db, cfg, cr, lastCfgFp)
	go master.StartMaintenance(ctx)
	go func() {
		if n := master.BackfillAssignments(ctx); n > 0 {
			log.Printf("已为 %d 个存量域名建立副本分配", n)
		}
	}()

	adminUser := envOr("QZ_ADMIN_USER", "admin")
	adminPass := envOr("QZ_ADMIN_PASS", "admin123")
	keys := auth.NewManager(db, cache)
	adminAuth := auth.NewAdminAuth(adminUser, adminPass, 24)

	srv := api.NewServer(cfg, db, cache, cr, indexer, searcher, master, keys, adminAuth, nil)

	// 子节点：先领取密钥，再按主地址表注册并保持心跳
	if cfg.Cluster.Role == "node" && len(cfg.Cluster.MasterAddrs) > 0 {
		agent := cluster.NewAgent(cfg, cfg.Cluster.AdvertiseAddr)
		srv.SetAgent(agent)
		// 归属表落库：节点库没有分配表会误把自己当所有域名的主副本，导致重复抓全站
		agent.SetOwnedApplier(master.SyncOwned)
		go func() {
			if agent.Secret() == "" {
				if err := agent.Enroll(ctx); err != nil {
					log.Printf("加入集群失败: %v", err)
				} else {
					log.Printf("已领取节点密钥，节点 ID=%s", cfg.Cluster.NodeID)
				}
			}
			report := func() {
				dc := countOwned(db, cfg.Cluster.NodeID)
				if err := agent.Register(ctx, dc, string(selfCfg)); err != nil {
					log.Printf("注册失败，尝试下一个主节点地址: %v", err)
				}
			}
			if err := agent.SyncOwned(ctx); err != nil {
				log.Printf("同步归属失败: %v", err)
			}
			report()

			interval := time.Duration(cfg.Cluster.HeartbeatSec) * time.Second
			if interval <= 0 {
				interval = 10 * time.Second
			}
			t := time.NewTicker(interval)
			defer t.Stop()
			prefer := time.NewTicker(60 * time.Second)
			defer prefer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-prefer.C:
					// 定期回到地址表首位，主入口恢复后切回去
					agent.ResetPreference()
					report()
				case <-t.C:
					if err := agent.SyncOwned(ctx); err != nil {
						log.Printf("同步归属失败: %v", err)
					}
					dc := countOwned(db, cfg.Cluster.NodeID)
					if err := agent.Heartbeat(ctx, dc, string(selfCfg)); err != nil {
						log.Printf("心跳失败: %v", err)
					}
				}
			}
		}()
	}

	// 自动开始抓取
	if cfg.Crawler.AutoStart {
		go func() {
			time.Sleep(1 * time.Second)
			ctx := context.Background()
			if _, err := cr.LoadSeeds(ctx); err != nil {
				log.Printf("加载种子失败: %v", err)
				return
			}
			cr.Run(ctx)
		}()
	}

	httpSrv := &http.Server{
		Addr:         cfg.Server.Addr,
		Handler:      srv.Handler(),
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeoutSec) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeoutSec) * time.Second,
	}

	go func() {
		log.Printf("========================================")
		log.Printf(" Queryza v%s 已启动  角色=%s 节点=%s", version, cfg.Cluster.Role, cfg.Cluster.NodeID)
		log.Printf(" 服务端口: %s", cfg.Server.Addr)
		log.Printf(" 管理后台: %s", cfg.Server.AdminPath)
		log.Printf(" 登录账号: %s / %s", adminUser, adminPass)
		log.Printf("========================================")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP 服务: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpSrv.Shutdown(ctx)
	log.Println("已退出")
}

// applyNodeOverride 读取后台保存的本节点独立配置并应用（启动时全量生效）。
// 返回该节点配置的指纹，供运行时轮询比对。
func applyNodeOverride(db *store.DB, cfg *config.Config, cr *crawler.Crawler) string {
	raw := db.GetConfig("node_override", "")
	if raw == "" {
		return ""
	}
	var all map[string]config.NodeOverride
	if err := json.Unmarshal([]byte(raw), &all); err != nil {
		return ""
	}
	o, ok := all[cfg.Cluster.NodeID]
	if !ok {
		return ""
	}
	cfg.ApplyValues(o)
	if cr != nil {
		cr.SetConcurrency(cfg.Crawler.Concurrency)
		cr.SetPerDomainIntervalMS(cfg.Crawler.PerDomainIntervalMS)
		cr.SetMaxPages(cfg.Crawler.MaxPagesPerRun)
	}
	log.Printf("已应用节点独立配置: 并发=%d 同域间隔=%dms 单次上限=%d",
		cfg.Crawler.Concurrency, cfg.Crawler.PerDomainIntervalMS, cfg.Crawler.MaxPagesPerRun)
	b, _ := json.Marshal(o)
	return string(b)
}

// watchNodeOverride 轮询后台下发的节点配置，只热更数值项，避免与抓取协程争用配置结构。
func watchNodeOverride(ctx context.Context, db *store.DB, cfg *config.Config, cr *crawler.Crawler, last string) {
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		raw := db.GetConfig("node_override", "")
		if raw == "" {
			continue
		}
		var all map[string]config.NodeOverride
		if json.Unmarshal([]byte(raw), &all) != nil {
			continue
		}
		o, ok := all[cfg.Cluster.NodeID]
		if !ok {
			continue
		}
		b, _ := json.Marshal(o)
		fp := string(b)
		if fp == last {
			continue
		}
		last = fp
		if cr != nil {
			if o.Concurrency != nil {
				cr.SetConcurrency(*o.Concurrency)
			}
			if o.PerDomainIntervalMS != nil {
				cr.SetPerDomainIntervalMS(*o.PerDomainIntervalMS)
			}
			if o.MaxPagesPerRun != nil {
				cr.SetMaxPages(*o.MaxPagesPerRun)
			}
		}
		log.Printf("节点配置已更新（后台下发）: %s", fp)
	}
}

func loadProxyGroup(db *store.DB, name string) []string {	if name == "" {
		return nil
	}
	raw := db.GetConfig("proxy_groups", "")
	if raw == "" {
		return nil
	}
	var groups map[string][]string
	if err := json.Unmarshal([]byte(raw), &groups); err != nil {
		return nil
	}
	return groups[name]
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func normalizePath(p string) string {
	if p == "" {
		return p
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return strings.TrimSuffix(p, "/")
}

// countOwned 统计本节点负责的域名下已入库的文档数
// countOwned 统计本节点已入库文档数。
// 节点库里的文档就是本节点自己抓的，不需要按 domain_replicas 过滤
// （节点库根本没有分配表，JOIN 会一直返回 0）。
func countOwned(db *store.DB, _ string) int64 {
	var n int64
	db.QueryRow(`SELECT COUNT(*) FROM documents`).Scan(&n)
	return n
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func randSuffix() string {
	const a = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	now := time.Now().UnixNano()
	for i := range b {
		b[i] = a[int(now>>uint(i*5))%len(a)]
	}
	return string(b)
}
