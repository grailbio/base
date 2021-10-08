package loadingcache

import "sync"

// Map is a keyed collection of Values. Map{} is ready to use.
// (*Map)(nil) is valid and never caches or shares results for any key.
// Maps are concurrency-safe. They must not be copied.
//
// Implementation notes:
//
// Compared to sync.Map, this Map is not sophisticated in terms of optimizing for high concurrency
// with disjoint sets of keys. It could probably be improved.
//
// Loading on-demand, without repeated value computation, is reminiscent of Guava's LoadingCache:
// https://github.com/google/guava/wiki/CachesExplained
type Map struct {
	mu sync.Mutex
	m  map[interface{}]*Value
}

// GetOrCreate returns an existing or new Value associated with key.
// Note: If m == nil, returns nil, a never-caching Value.
func (m *Map) GetOrCreate(key interface{}) *Value {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.m == nil {
		m.m = make(map[interface{}]*Value)
	}
	if _, ok := m.m[key]; !ok {
		m.m[key] = new(Value)
	}
	return m.m[key]
}

func (m *Map) DeleteAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m = nil
}
