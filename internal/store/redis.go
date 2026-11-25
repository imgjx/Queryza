package store

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache struct{ *redis.Client }

func OpenRedis(addr, pass string, db, pool int) (*Cache, error) {
	c := redis.NewClient(&redis.Options{Addr: addr, Password: pass, DB: db, PoolSize: pool})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return &Cache{c}, nil
}

func (c *Cache) SetJSON(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return c.Set(ctx, key, val, ttl).Err()
}

func (c *Cache) GetJSON(ctx context.Context, key string) ([]byte, bool) {
	b, err := c.Get(ctx, key).Bytes()
	if err != nil {
		return nil, false
	}
	return b, true
}

func (c *Cache) IncrHot(ctx context.Context, query string) {
	key := "qz:hot:" + time.Now().Format("2006-01-02")
	c.ZIncrBy(ctx, key, 1, query)
	c.Expire(ctx, key, 48*time.Hour)
}

func (c *Cache) IncrWithTTL(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	n, err := c.Client.Incr(ctx, key).Result()
	if err == nil && n == 1 && ttl > 0 {
		c.Client.Expire(ctx, key, ttl)
	}
	return n, err
}
