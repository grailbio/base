package s3transport

import (
	"net"
	"sync"
	"time"
)

const dnsCacheTime = 5 * time.Second

type resolverCacheEntry struct {
	result     []net.IP
	resolvedAt time.Time
}

type resolver struct {
	lookupIP func(host string) ([]net.IP, error)
	now      func() time.Time
	cacheMu  sync.Mutex
	cache    map[string]resolverCacheEntry
}

func newResolver(lookupIP func(host string) ([]net.IP, error), now func() time.Time) *resolver {
	return &resolver{
		lookupIP: lookupIP,
		now:      now,
		cache:    map[string]resolverCacheEntry{},
	}
}

var defaultResolver = newResolver(net.LookupIP, time.Now)

func (r *resolver) LookupIP(host string) ([]net.IP, error) {
	r.cacheMu.Lock()
	entry, ok := r.cache[host]
	r.cacheMu.Unlock()
	now := r.now()
	if ok && now.Sub(entry.resolvedAt) < dnsCacheTime {
		return entry.result, nil
	}
	ips, err := r.lookupIP(host)
	if err != nil {
		return nil, err
	}
	r.cacheMu.Lock()
	r.cache[host] = resolverCacheEntry{ips, now}
	r.cacheMu.Unlock()
	return ips, nil
}
