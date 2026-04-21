package redis

import "github.com/redis/go-redis/v9"

type RedisClient struct {
	*redis.Client
}

func NewRedisClient(cfg *redis.Options) *RedisClient {
	return &RedisClient{
		Client: redis.NewClient(cfg),
	}
}

func (c *RedisClient) Close() error {
	return c.Client.Close()
}
