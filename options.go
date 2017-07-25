package cache

import (
	"time"
)

type Options struct {
	Name             string
	Engine           string
	RedisOptions     *RedisOptions
	MemcachedOptions *MemcachedOptions
	Logger           ILogger
}

type RedisOptions struct {
	ClusterEnable bool
	Addresses     []string
	Credential    string
	DB            int
	PoolSize      int
	PoolTimeout   time.Duration
	IdleTimeout   time.Duration
}

type MemcachedOptions struct {
	Addresses []string
}
