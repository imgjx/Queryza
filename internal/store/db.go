package store

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type DB struct{ *sql.DB }

func OpenDB(dsn string, maxOpen, maxIdle, lifeSec int) (*DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if maxOpen <= 0 {
		maxOpen = 32
	}
	if maxIdle <= 0 {
		maxIdle = 8
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(time.Duration(lifeSec) * time.Second)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("连接数据库失败: %w", err)
	}
	return &DB{db}, nil
}

var schema = []string{
	`CREATE TABLE IF NOT EXISTS documents (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		url VARCHAR(768) NOT NULL,
		url_hash CHAR(64) NOT NULL,
		domain VARCHAR(255) NOT NULL,
		title VARCHAR(512) NOT NULL DEFAULT '',
		content MEDIUMTEXT,
		content_len INT NOT NULL DEFAULT 0,
		http_status INT NOT NULL DEFAULT 0,
		fetched_at DATETIME NOT NULL,
		content_hash CHAR(64) NOT NULL DEFAULT '',
		simhash BIGINT NOT NULL DEFAULT 0,
		embedding LONGBLOB,
		embedding_dim INT NOT NULL DEFAULT 0,
		site_weight DOUBLE NOT NULL DEFAULT 1,
		dynamic_weight DOUBLE NOT NULL DEFAULT 1,
		trust_score DOUBLE NOT NULL DEFAULT 0,
		spam_score DOUBLE NOT NULL DEFAULT 0,
		is_hot TINYINT NOT NULL DEFAULT 0,
		index_status TINYINT NOT NULL DEFAULT 1,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		UNIQUE KEY uk_url_hash (url_hash),
		KEY idx_domain (domain),
		KEY idx_content_hash (content_hash),
		KEY idx_fetched (fetched_at),
		KEY idx_hot (is_hot),
		KEY idx_simhash (simhash)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

	`CREATE TABLE IF NOT EXISTS postings (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		term VARCHAR(80) NOT NULL,
		doc_id BIGINT NOT NULL,
		tf INT NOT NULL DEFAULT 1,
		UNIQUE KEY uk_term_doc (term, doc_id),
		KEY idx_doc (doc_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

	`CREATE TABLE IF NOT EXISTS terms (
		term VARCHAR(80) NOT NULL PRIMARY KEY,
		df INT NOT NULL DEFAULT 0
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

	`CREATE TABLE IF NOT EXISTS seeds (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		url VARCHAR(768) NOT NULL,
		url_hash CHAR(64) NOT NULL,
		domain VARCHAR(255) NOT NULL,
		name VARCHAR(255) NOT NULL DEFAULT '',
		category VARCHAR(64) NOT NULL DEFAULT '',
		weight DOUBLE NOT NULL DEFAULT 1,
		enabled TINYINT NOT NULL DEFAULT 1,
		depth INT NOT NULL DEFAULT 2,
		last_crawled_at DATETIME NULL,
		UNIQUE KEY uk_url_hash (url_hash),
		KEY idx_domain (domain)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

	`CREATE TABLE IF NOT EXISTS nodes (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		node_id VARCHAR(64) NOT NULL,
		name VARCHAR(128) NOT NULL DEFAULT '',
		addr VARCHAR(255) NOT NULL DEFAULT '',
		role VARCHAR(32) NOT NULL DEFAULT 'node',
		status VARCHAR(32) NOT NULL DEFAULT 'unknown',
		weight DOUBLE NOT NULL DEFAULT 1,
		doc_count BIGINT NOT NULL DEFAULT 0,
		last_heartbeat DATETIME NULL,
		is_self TINYINT NOT NULL DEFAULT 0,
		config MEDIUMTEXT,
		UNIQUE KEY uk_node_id (node_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

	`CREATE TABLE IF NOT EXISTS domain_assignments (
		domain VARCHAR(255) NOT NULL PRIMARY KEY,
		node_id VARCHAR(64) NOT NULL,
		weight DOUBLE NOT NULL DEFAULT 1,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

	// 域名副本：同一域名可落在多个节点，role=primary 的副本对外提供检索
	`CREATE TABLE IF NOT EXISTS domain_replicas (
		domain VARCHAR(255) NOT NULL,
		shard INT NOT NULL DEFAULT 0,
		node_id VARCHAR(64) NOT NULL,
		role VARCHAR(16) NOT NULL DEFAULT 'replica',
		weight DOUBLE NOT NULL DEFAULT 1,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (domain, shard, node_id),
		KEY idx_node (node_id),
		KEY idx_role (role)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

	// 域名分片策略：某个域名文档量太大时拆成多个分片，分片落在不同节点
	`CREATE TABLE IF NOT EXISTS domain_shards (
		domain VARCHAR(255) NOT NULL,
		shards INT NOT NULL DEFAULT 1,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (domain)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

	// 节点加入令牌：一次性，短期有效
	`CREATE TABLE IF NOT EXISTS node_tokens (
		token CHAR(48) NOT NULL PRIMARY KEY,
		node_id VARCHAR(64) NOT NULL,
		expires_at DATETIME NOT NULL,
		used TINYINT NOT NULL DEFAULT 0,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		KEY idx_node (node_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

	`CREATE TABLE IF NOT EXISTS api_keys (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		key_hash CHAR(64) NOT NULL,
		key_prefix VARCHAR(16) NOT NULL DEFAULT '',
		name VARCHAR(128) NOT NULL DEFAULT '',
		qps_limit INT NOT NULL DEFAULT 10,
		daily_quota INT NOT NULL DEFAULT 10000,
		daily_used INT NOT NULL DEFAULT 0,
		result_max INT NOT NULL DEFAULT 50,
		enabled TINYINT NOT NULL DEFAULT 1,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		last_used_at DATETIME NULL,
		UNIQUE KEY uk_key_hash (key_hash)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

	`CREATE TABLE IF NOT EXISTS crawl_queue (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		url VARCHAR(768) NOT NULL,
		url_hash CHAR(64) NOT NULL,
		domain VARCHAR(255) NOT NULL,
		shard INT NOT NULL DEFAULT 0,
		depth INT NOT NULL DEFAULT 0,
		priority INT NOT NULL DEFAULT 0,
		status VARCHAR(24) NOT NULL DEFAULT 'pending',
		retries INT NOT NULL DEFAULT 0,
		node_id VARCHAR(64) NOT NULL DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		UNIQUE KEY uk_url_hash (url_hash),
		KEY idx_status_prio (status, priority),
		KEY idx_status_domain (status, domain),
		KEY idx_domain (domain)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

	`CREATE TABLE IF NOT EXISTS crawl_logs (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		url VARCHAR(768) NOT NULL,
		domain VARCHAR(255) NOT NULL DEFAULT '',
		status VARCHAR(24) NOT NULL DEFAULT '',
		http_code INT NOT NULL DEFAULT 0,
		bytes INT NOT NULL DEFAULT 0,
		error VARCHAR(512) NOT NULL DEFAULT '',
		duration_ms INT NOT NULL DEFAULT 0,
		node_id VARCHAR(64) NOT NULL DEFAULT '',
		rule_id VARCHAR(32) NOT NULL DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		KEY idx_created (created_at),
		KEY idx_domain (domain)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

	`CREATE TABLE IF NOT EXISTS query_logs (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		query VARCHAR(512) NOT NULL,
		results INT NOT NULL DEFAULT 0,
		latency_ms INT NOT NULL DEFAULT 0,
		key_id BIGINT NOT NULL DEFAULT 0,
		node_id VARCHAR(64) NOT NULL DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		KEY idx_created (created_at)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,

	`CREATE TABLE IF NOT EXISTS system_config (
		k VARCHAR(128) NOT NULL PRIMARY KEY,
		v MEDIUMTEXT,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
}

func (d *DB) Migrate() error {
	for _, s := range schema {
		if _, err := d.Exec(s); err != nil {
			return fmt.Errorf("建表失败: %w", err)
		}
	}
	// 增量索引：按域名轮转领取任务需要 (status,domain) 索引
	d.ensureIndex("crawl_queue", "idx_status_domain", "KEY idx_status_domain (status, domain)")
	d.ensureIndex("documents", "idx_content_hash", "KEY idx_content_hash (content_hash)")
	// 分片检索按 (domain,shard) 过滤
	d.ensureIndex("documents", "idx_domain_shard", "KEY idx_domain_shard (domain, shard)")

	// 增量列：老库升级时补齐
	for _, c := range []struct{ table, col, ddl string }{
		{"nodes", "secret", "secret VARCHAR(64) NOT NULL DEFAULT ''"},
		{"nodes", "enabled", "enabled TINYINT NOT NULL DEFAULT 1"},
		{"nodes", "version", "version VARCHAR(32) NOT NULL DEFAULT ''"},
		{"nodes", "last_ip", "last_ip VARCHAR(64) NOT NULL DEFAULT ''"},
		{"crawl_logs", "rule_id", "rule_id VARCHAR(32) NOT NULL DEFAULT ''"},
		{"documents", "shard", "shard INT NOT NULL DEFAULT 0"},
		{"crawl_queue", "shard", "shard INT NOT NULL DEFAULT 0"},
		{"domain_replicas", "shard", "shard INT NOT NULL DEFAULT 0"},
	} {
		d.ensureColumn(c.table, c.col, c.ddl)
	}
	// 倒排表主键改造：以 (term,doc_id) 聚簇，去掉自增 id 与冗余唯一索引。
	// 写入变成按词项顺序追加，表也更小；只需迁移一次。
	d.migratePostingsPK()
	// 副本表主键加入分片维度
	d.migrateReplicaPK()

	// 旧版单副本分配表迁移到多副本表（幂等）
	d.Exec(`INSERT IGNORE INTO domain_replicas(domain,node_id,role,weight)
		SELECT domain,node_id,'primary',weight FROM domain_assignments`)
	return nil
}

func (d *DB) migratePostingsPK() {
	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='postings' AND COLUMN_NAME='id'`).Scan(&n); err != nil {
		return
	}
	if n == 0 {
		return
	}
	d.Exec(`ALTER TABLE postings DROP PRIMARY KEY, DROP KEY uk_term_doc, DROP COLUMN id, ADD PRIMARY KEY(term,doc_id)`)
}

func (d *DB) migrateReplicaPK() {
	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='domain_replicas'
		  AND INDEX_NAME='PRIMARY' AND COLUMN_NAME='shard'`).Scan(&n); err != nil {
		return
	}
	if n > 0 {
		return
	}
	d.Exec(`ALTER TABLE domain_replicas DROP PRIMARY KEY, ADD PRIMARY KEY(domain, shard, node_id)`)
}

func (d *DB) ensureIndex(table, name, ddl string) {
	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND INDEX_NAME=?`, table, name).Scan(&n); err != nil {
		return
	}
	if n == 0 {
		d.Exec("ALTER TABLE " + table + " ADD " + ddl)
	}
}

func (d *DB) ensureColumn(table, col, ddl string) {
	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=?`, table, col).Scan(&n); err != nil {
		return
	}
	if n == 0 {
		d.Exec("ALTER TABLE " + table + " ADD COLUMN " + ddl)
	}
}

func (d *DB) SetConfig(k, v string) error {
	_, err := d.Exec(`INSERT INTO system_config(k,v) VALUES(?,?) ON DUPLICATE KEY UPDATE v=VALUES(v)`, k, v)
	return err
}

func (d *DB) GetConfig(k string, def string) string {
	var v string
	err := d.QueryRow(`SELECT v FROM system_config WHERE k=?`, k).Scan(&v)
	if err != nil {
		return def
	}
	return v
}
