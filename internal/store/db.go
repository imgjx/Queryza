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
		depth INT NOT NULL DEFAULT 0,
		priority INT NOT NULL DEFAULT 0,
		status VARCHAR(24) NOT NULL DEFAULT 'pending',
		retries INT NOT NULL DEFAULT 0,
		node_id VARCHAR(64) NOT NULL DEFAULT '',
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		UNIQUE KEY uk_url_hash (url_hash),
		KEY idx_status_prio (status, priority),
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
	return nil
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
