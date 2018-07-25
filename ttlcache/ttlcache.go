// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.
//
// Package ttlcache implements a cache with a fixed TTL. The keys and values
// are interface{} and the TTL for an item starts decreasing each time the item
// is added to the cache.
//
// There is no active garbage collection but expired items are deleted
// from the cache upon new 'Get' calls. This is a lazy strategy that
// does not prevent memory leaks.

package ttlcache

import (
	"sync"
	"time"
)

type cacheValue struct {
	value      interface{}
	expiration time.Time
}

type Cache struct {
	cache map[interface{}]cacheValue
	mu    sync.Mutex
	ttl   time.Duration
}

func New(ttl time.Duration) *Cache {
	return &Cache{
		cache: map[interface{}]cacheValue{},
		ttl:   ttl,
	}
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.mu.Lock()
	v, ok := c.cache[key]

	if ok && v.expiration.After(time.Now()) {
		c.mu.Unlock()
		return v.value, true
	}
	if ok {
		delete(c.cache, key) // key is expired - delete it.
	}
	c.mu.Unlock()
	return nil, false
}

func (c *Cache) Set(key interface{}, value interface{}) {
	c.mu.Lock()
	c.cache[key] = cacheValue{
		value:      value,
		expiration: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
}
