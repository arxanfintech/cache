package cache

import (
	"fmt"
	"time"

	"gopkg.in/redis.v4"
)

type RedisClient struct {
	redisClient        *redis.Client
	redisClusterClient *redis.ClusterClient
	redisCluster       bool
}

func NewRedisClient(opts *RedisOptions) (*RedisClient, error) {
	if opts == nil {
		return nil, fmt.Errorf("invalid input options")
	}

	this := new(RedisClient)

	err := this.init(opts)
	if err != nil {
		return nil, err
	}

	return this, nil
}

// InitStorage Implement storage.InitStorage()
func (r *RedisClient) init(opts *RedisOptions) error {

	r.redisCluster = opts.ClusterEnable

	r.Close()

	var cmd *redis.StatusCmd
	if !r.redisCluster {

		r.redisClient = redis.NewClient(&redis.Options{
			Addr:        opts.Addresses[0],
			Password:    opts.Credential,
			DB:          opts.DB,
			PoolSize:    opts.PoolSize,
			PoolTimeout: opts.PoolTimeout,
			IdleTimeout: opts.IdleTimeout,
		})

		cmd = r.redisClient.Ping()

	} else {

		r.redisClusterClient = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:       opts.Addresses,
			Password:    opts.Credential,
			PoolSize:    opts.PoolSize,
			PoolTimeout: opts.PoolTimeout,
			IdleTimeout: opts.IdleTimeout,
		})

		cmd = r.redisClusterClient.Ping()
	}

	return cmd.Err()
}

// Implement storage.Close()
func (r *RedisClient) Close() {
	if r.redisClient != nil {
		r.redisClient.Close()
		r.redisClient = nil
	}
	if r.redisClusterClient != nil {
		r.redisClusterClient.Close()
		r.redisClusterClient = nil
	}
}

func (r *RedisClient) SetString(key, value string, expiration time.Duration) (string, error) {
	var cmd *redis.StatusCmd
	if !r.redisCluster {
		cmd = r.redisClient.Set(key, value, expiration)
	} else {
		cmd = r.redisClusterClient.Set(key, value, expiration)
	}
	return cmd.Result()
}

func (r *RedisClient) SetAdd(key, member string) error {
	var cmd *redis.IntCmd
	if !r.redisCluster {
		cmd = r.redisClient.SAdd(key, member)
	} else {
		cmd = r.redisClusterClient.SAdd(key, member)
	}
	return cmd.Err()
}

func (r *RedisClient) ListPush(key, value string, removeDup bool) error {
	var cmd *redis.IntCmd
	if !r.redisCluster {
		if removeDup {
			// remove duplicate value
			r.redisClient.LRem(key, 0, value)
		}
		cmd = r.redisClient.LPush(key, value)
	} else {
		if removeDup {
			// remove duplicate value
			r.redisClusterClient.LRem(key, 0, value)
		}
		cmd = r.redisClusterClient.LPush(key, value)
	}
	return cmd.Err()
}

func (r *RedisClient) ListPop(key string) (result string, err error) {
	var cmd *redis.StringCmd
	if !r.redisCluster {
		cmd = r.redisClient.RPop(key)
	} else {
		cmd = r.redisClusterClient.RPop(key)
	}
	return cmd.Result()
}

func (r *RedisClient) ListKeys(keyPattern string) (results []string, err error) {
	cmd := r.redisClient.Keys(keyPattern)
	return cmd.Result()
}

func (r *RedisClient) ListSize(key string) (size int64, err error) {
	var cmd *redis.IntCmd
	if !r.redisCluster {
		cmd = r.redisClient.LLen(key)
	} else {
		cmd = r.redisClusterClient.LLen(key)
	}
	return cmd.Result()
}

func (r *RedisClient) ListRPopLPush(srcKey, targetKey string) (result string, err error) {
	var cmd *redis.StringCmd
	if !r.redisCluster {
		cmd = r.redisClient.RPopLPush(srcKey, targetKey)
	} else {
		cmd = r.redisClusterClient.RPopLPush(srcKey, targetKey)
	}
	return cmd.Result()
}

func (r *RedisClient) ListRange(key string, from, to int64) (result []string, err error) {
	var cmd *redis.StringSliceCmd
	if !r.redisCluster {
		cmd = r.redisClient.LRange(key, from, to)
	} else {
		cmd = r.redisClusterClient.LRange(key, from, to)
	}
	return cmd.Result()
}

func (r *RedisClient) DelKey(key string) (int64, error) {
	var cmd *redis.IntCmd
	if !r.redisCluster {
		cmd = r.redisClient.Del(key)
	} else {
		cmd = r.redisClusterClient.Del(key)
	}
	return cmd.Result()
}

func (r *RedisClient) HGet(key, field string) ([]byte, error) {

	var cmd *redis.StringCmd
	if !r.redisCluster {
		cmd = r.redisClient.HGet(key, field)
	} else {
		cmd = r.redisClusterClient.HGet(key, field)
	}
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	return cmd.Bytes()
}

func (r *RedisClient) HMSet(key string, fields map[string]interface{}) error {
	toStr := make(map[string]string)
	for k, v := range fields {
		toStr[k] = fmt.Sprint(v)
	}
	var cmd *redis.StatusCmd
	if !r.redisCluster {
		cmd = r.redisClient.HMSet(key, toStr)
	} else {
		cmd = r.redisClusterClient.HMSet(key, toStr)
	}
	return cmd.Err()
}

func (r *RedisClient) HSet(key, field string, data []byte) error {

	var cmd *redis.BoolCmd
	if !r.redisCluster {
		cmd = r.redisClient.HSet(key, field, string(data))
	} else {
		cmd = r.redisClusterClient.HSet(key, field, string(data))
	}
	return cmd.Err()
}

func (r *RedisClient) HDel(key string, fields ...string) error {

	var cmd *redis.IntCmd
	if !r.redisCluster {
		cmd = r.redisClient.HDel(key, fields...)
	} else {
		cmd = r.redisClusterClient.HDel(key, fields...)
	}
	return cmd.Err()
}
