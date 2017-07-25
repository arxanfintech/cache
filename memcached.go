package cache

import (
	"fmt"

	"github.com/bradfitz/gomemcache/memcache"
)

type MemcachedClient struct {
	mcClient *memcache.Client
}

func NewMemcachedClient(opts *MemcachedOptions) (*MemcachedClient, error) {
	if opts == nil {
		return nil, fmt.Errorf("invalid input options")
	}

	this := new(MemcachedClient)

	this.mcClient = memcache.New(opts.Addresses...)

	return this, nil
}

func (m *MemcachedClient) HGet(keyPrefix, field string) ([]byte, error) {
	if m.mcClient == nil {
		return nil, fmt.Errorf("memcached client instance invalid")
	}

	k := fmt.Sprintf("%s_%s", keyPrefix, field)
	it, err := m.mcClient.Get(k)
	if err != nil {
		return nil, err
	}
	return it.Value, nil
}

func (m *MemcachedClient) HSet(keyPrefix, field string, data []byte) error {
	if m.mcClient == nil {
		return fmt.Errorf("memcached client instance invalid")
	}

	k := fmt.Sprintf("%s_%s", keyPrefix, field)
	return m.mcClient.Set(&memcache.Item{Key: k, Value: data})
}

func (m *MemcachedClient) HDel(keyPrefix string, fields ...string) error {
	if m.mcClient == nil {
		return fmt.Errorf("memcached client instance invalid")
	}

	var err error
	var k string
	for _, field := range fields {
		k = fmt.Sprintf("%s_%s", keyPrefix, field)
		err = m.mcClient.Delete(k)
		if err == memcache.ErrCacheMiss { //the item didn't already exist in the cache
			err = nil
		}
	}
	return err
}
