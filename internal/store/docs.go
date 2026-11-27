package store

import (
	"context"
	"strings"
	"time"
)

// DocRow 文档库列表项（不含正文，避免一次拉几万篇把页面拖死）。
type DocRow struct {
	ID            int64   `json:"id"`
	URL           string  `json:"url"`
	Title         string  `json:"title"`
	Domain        string  `json:"domain"`
	ContentLen    int     `json:"content_len"`
	HTTPStatus    int     `json:"http_status"`
	FetchedAt     string  `json:"fetched_at"`
	SiteWeight    float64 `json:"site_weight"`
	DynamicWeight float64 `json:"dynamic_weight"`
	TrustScore    float64 `json:"trust_score"`
	SpamScore     float64 `json:"spam_score"`
	IndexStatus   int     `json:"index_status"`
	Shard         int     `json:"shard"`
}

// DocQuery 文档库查询条件。Shard < 0 表示不限分片。
type DocQuery struct {
	Page   int
	Size   int
	Q      string
	Domain string
	Shard  int
}

// QueryDocs 分页查询文档库。返回当页数据与总条数。
func (d *DB) QueryDocs(ctx context.Context, q DocQuery) ([]DocRow, int64, error) {
	if q.Page < 1 {
		q.Page = 1
	}
	if q.Size < 1 || q.Size > 200 {
		q.Size = 20
	}
	var conds []string
	var args []interface{}
	if kw := strings.TrimSpace(q.Q); kw != "" {
		like := "%" + kw + "%"
		conds = append(conds, "(title LIKE ? OR url LIKE ? OR domain LIKE ?)")
		args = append(args, like, like, like)
	}
	if q.Domain != "" {
		conds = append(conds, "domain = ?")
		args = append(args, q.Domain)
	}
	if q.Shard >= 0 {
		conds = append(conds, "shard = ?")
		args = append(args, q.Shard)
	}
	where := ""
	if len(conds) > 0 {
		where = " WHERE " + strings.Join(conds, " AND ")
	}

	var total int64
	if err := d.QueryRowContext(ctx, `SELECT COUNT(*) FROM documents`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := d.QueryContext(ctx, `SELECT id,url,title,domain,content_len,http_status,fetched_at,
		site_weight,dynamic_weight,trust_score,spam_score,index_status,shard
		FROM documents`+where+` ORDER BY id DESC LIMIT ? OFFSET ?`,
		append(args, q.Size, (q.Page-1)*q.Size)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	out := make([]DocRow, 0, q.Size)
	for rows.Next() {
		var r DocRow
		var fetched time.Time
		if rows.Scan(&r.ID, &r.URL, &r.Title, &r.Domain, &r.ContentLen, &r.HTTPStatus, &fetched,
			&r.SiteWeight, &r.DynamicWeight, &r.TrustScore, &r.SpamScore, &r.IndexStatus, &r.Shard) == nil {
			r.FetchedAt = fetched.Format("2006-01-02 15:04")
			out = append(out, r)
		}
	}
	return out, total, nil
}

// DocStats 文档库概览：总量、域名数、平均长度、最近一天新增。
type DocStats struct {
	Total     int64 `json:"total"`
	Domains   int64 `json:"domains"`
	AvgLen    int64 `json:"avg_len"`
	Today     int64 `json:"today"`
	Indexed   int64 `json:"indexed"`
	RedisDocs int64 `json:"redis_docs"`
}

// Stats 统计本库文档规模，供后台展示。
func (d *DB) Stats(ctx context.Context) DocStats {
	var st DocStats
	d.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(DISTINCT domain),
		CAST(COALESCE(AVG(content_len),0) AS SIGNED),
		CAST(COALESCE(SUM(created_at >= CURDATE()),0) AS SIGNED),
		CAST(COALESCE(SUM(index_status=1),0) AS SIGNED) FROM documents`).
		Scan(&st.Total, &st.Domains, &st.AvgLen, &st.Today, &st.Indexed)
	return st
}

// TopDomains 返回文档量最大的域名，便于观察分片与分配是否均衡。
type DomainCount struct {
	Domain string `json:"domain"`
	Docs   int64  `json:"docs"`
}

func (d *DB) TopDomains(ctx context.Context, limit int) []DomainCount {
	if limit <= 0 || limit > 200 {
		limit = 20
	}
	rows, err := d.QueryContext(ctx, `SELECT domain,COUNT(*) c FROM documents
		GROUP BY domain ORDER BY c DESC LIMIT ?`, limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []DomainCount
	for rows.Next() {
		var x DomainCount
		if rows.Scan(&x.Domain, &x.Docs) == nil {
			out = append(out, x)
		}
	}
	return out
}
