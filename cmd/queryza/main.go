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
	masterAddr := flag.String("master", "", "主节点地址（node 角色）")
	advertise := flag.String("advertise", "", "本节点对外地址（用于注册）")
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
		cfg.Cluster.MasterAddr = *masterAddr
	}
	if *advertise != "" {
		cfg.Cluster.AdvertiseAddr = *advertise
	}
	if cfg.Cluster.AdvertiseAddr == "" {
		cfg.Cluster.AdvertiseAddr = "http://127.0.0.1" + cfg.Server.Addr
	}
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

	proxies := loadProxyGroup(db, cfg.Crawler.ProxyGroup)
	cr := crawler.New(cfg, db, cache, cfg.Cluster.NodeID, proxies, indexer)

	master := cluster.NewMaster(cfg, db, searcher)
	selfCfg, _ := json.Marshal(cfg.SelfConfig())
	master.Register(cluster.RegisterReq{
		NodeID: cfg.Cluster.NodeID, Name: cfg.Cluster.NodeName, Addr: cfg.Cluster.AdvertiseAddr,
		Role: cfg.Cluster.Role, Weight: cfg.Cluster.Weight, DocCount: 0, Config: string(selfCfg),
	})

	adminUser := envOr("QZ_ADMIN_USER", "admin")
	adminPass := envOr("QZ_ADMIN_PASS", "admin123")
	keys := auth.NewManager(db, cache)
	adminAuth := auth.NewAdminAuth(adminUser, adminPass, 24)

	srv := api.NewServer(cfg, db, cache, cr, indexer, searcher, master, keys, adminAuth, nil)

	// 子节点：注册并保持心跳
	if cfg.Cluster.Role == "node" && cfg.Cluster.MasterAddr != "" {
		agent := cluster.NewAgent(cfg, cfg.Cluster.AdvertiseAddr)
		go func() {
			ctx := context.Background()
			if err := agent.Register(ctx, 0, string(selfCfg)); err != nil {
				log.Printf("注册到主节点失败: %v", err)
			} else {
				log.Printf("已注册到主节点 %s", cfg.Cluster.MasterAddr)
			}
			interval := time.Duration(cfg.Cluster.HeartbeatSec) * time.Second
			if interval <= 0 {
				interval = 10 * time.Second
			}
			t := time.NewTicker(interval)
			defer t.Stop()
			for range t.C {
				var dc int64
				db.QueryRow(`SELECT COUNT(*) FROM documents`).Scan(&dc)
				agent.Heartbeat(context.Background(), dc, string(selfCfg))
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

func loadProxyGroup(db *store.DB, name string) []string {
	if name == "" {
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

func randSuffix() string {
	const a = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	now := time.Now().UnixNano()
	for i := range b {
		b[i] = a[int(now>>uint(i*5))%len(a)]
	}
	return string(b)
}
