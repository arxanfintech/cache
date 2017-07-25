package cache

import (
	"fmt"
	"strings"
)

const (
	CACHE_ENGINE_REDIS     = "redis"
	CACHE_ENGINE_MEMCACHED = "memcached"
)

type ICacheClient interface {
	HGet(key, field string) ([]byte, error)
	HSet(key, field string, data []byte) error
	HDel(key string, fields ...string) error
}

type CacheClient struct {
	logger ILogger
	key    string
	client ICacheClient
}

func NewCacheClient(opts *Options) (*CacheClient, error) {
	this := &CacheClient{}

	if opts == nil {
		return nil, fmt.Errorf("invalid input options")
	}
	if opts.Engine == "" {
		opts.Engine = CACHE_ENGINE_MEMCACHED
	}
	if opts.Name == "" {
		opts.Name = "tmp" //TODO: random generate one name
	}

	this.logger = opts.Logger
	this.key = fmt.Sprintf("%s_CACHE", strings.ToUpper(opts.Name))

	if this.logger != nil {
		this.logger.Debugf("Using the cache engine: %s", opts.Engine)
	}

	var err error
	switch opts.Engine {
	case CACHE_ENGINE_REDIS:
		this.client, err = NewRedisClient(opts.RedisOptions)
	case CACHE_ENGINE_MEMCACHED:
		this.client, err = NewMemcachedClient(opts.MemcachedOptions)
	default:
		return nil, fmt.Errorf("cache engine not supported: %s", opts.Engine)
	}
	if err != nil {
		if this.logger != nil {
			this.logger.Errorf("New cache client fail: %v", err)
		}
		return nil, err
	}

	return this, nil
}

func (this *CacheClient) Get(field string) (data []byte, err error) {
	return this.client.HGet(this.key, field)
}

func (this *CacheClient) Set(field string, data []byte) (err error) {
	return this.client.HSet(this.key, field, data)
}

func (this *CacheClient) Clean(fields []string) (err error) {
	return this.client.HDel(this.key, fields...)
}
