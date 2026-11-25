package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"queryza/internal/store"
)

type Manager struct {
	db    *store.DB
	cache *store.Cache
}

func NewManager(db *store.DB, cache *store.Cache) *Manager {
	return &Manager{db: db, cache: cache}
}

func hashKey(k string) string {
	h := sha256.Sum256([]byte(k))
	return hex.EncodeToString(h[:])
}

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (m *Manager) Create(ctx context.Context, name string, qps, daily, resultMax int) (string, *store.APIKey, error) {
	if qps <= 0 {
		qps = 10
	}
	if daily <= 0 {
		daily = 10000
	}
	if resultMax <= 0 {
		resultMax = 50
	}
	raw := "qz_" + randomHex(20)
	h := hashKey(raw)
	prefix := raw[:10]
	res, err := m.db.ExecContext(ctx, `INSERT INTO api_keys(key_hash,key_prefix,name,qps_limit,daily_quota,result_max,enabled)
		VALUES(?,?,?,?,?,?,1)`, h, prefix, name, qps, daily, resultMax)
	if err != nil {
		return "", nil, err
	}
	id, _ := res.LastInsertId()
	return raw, &store.APIKey{
		ID: id, KeyPrefix: prefix, Name: name, QPSLimit: qps, DailyQuota: daily,
		ResultMax: resultMax, Enabled: true, CreatedAt: time.Now(),
	}, nil
}

var (
	ErrInvalidKey = fmt.Errorf("API Key 无效")
	ErrDisabled   = fmt.Errorf("API Key 已禁用")
	ErrQPS        = fmt.Errorf("超出 QPS 限制")
	ErrQuota      = fmt.Errorf("超出每日调用配额")
)

func (m *Manager) Verify(ctx context.Context, raw string) (*store.APIKey, error) {
	if raw == "" {
		return nil, ErrInvalidKey
	}
	h := hashKey(raw)
	k := &store.APIKey{}
	var lastUsed *time.Time
	err := m.db.QueryRowContext(ctx, `SELECT id,key_prefix,name,qps_limit,daily_quota,daily_used,result_max,enabled,created_at,last_used_at
		FROM api_keys WHERE key_hash=?`, h).Scan(&k.ID, &k.KeyPrefix, &k.Name, &k.QPSLimit, &k.DailyQuota,
		&k.DailyUsed, &k.ResultMax, &k.Enabled, &k.CreatedAt, &lastUsed)
	if err != nil {
		return nil, ErrInvalidKey
	}
	k.LastUsedAt = lastUsed
	if !k.Enabled {
		return nil, ErrDisabled
	}
	if m.cache != nil {
		sec := time.Now().Unix()
		qk := fmt.Sprintf("qz:qps:%d:%d", k.ID, sec)
		n, err := m.cache.IncrWithTTL(ctx, qk, 2*time.Second)
		if err == nil && k.QPSLimit > 0 && n > int64(k.QPSLimit) {
			return nil, ErrQPS
		}
		day := time.Now().Format("2006-01-02")
		dk := fmt.Sprintf("qz:daily:%d:%s", k.ID, day)
		dn, err := m.cache.IncrWithTTL(ctx, dk, 48*time.Hour)
		if err == nil && k.DailyQuota > 0 && dn > int64(k.DailyQuota) {
			return nil, ErrQuota
		}
	}
	m.db.ExecContext(ctx, `UPDATE api_keys SET last_used_at=NOW(), daily_used=daily_used+1 WHERE id=?`, k.ID)
	return k, nil
}

func (m *Manager) List(ctx context.Context) ([]store.APIKey, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT id,key_prefix,name,qps_limit,daily_quota,daily_used,result_max,enabled,created_at,last_used_at
		FROM api_keys ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.APIKey
	for rows.Next() {
		var k store.APIKey
		var lastUsed *time.Time
		if rows.Scan(&k.ID, &k.KeyPrefix, &k.Name, &k.QPSLimit, &k.DailyQuota, &k.DailyUsed,
			&k.ResultMax, &k.Enabled, &k.CreatedAt, &lastUsed) == nil {
			k.LastUsedAt = lastUsed
			out = append(out, k)
		}
	}
	return out, nil
}

func (m *Manager) SetEnabled(ctx context.Context, id int64, enabled bool) error {
	_, err := m.db.ExecContext(ctx, `UPDATE api_keys SET enabled=? WHERE id=?`, enabled, id)
	return err
}

func (m *Manager) Delete(ctx context.Context, id int64) error {
	_, err := m.db.ExecContext(ctx, `DELETE FROM api_keys WHERE id=?`, id)
	return err
}
