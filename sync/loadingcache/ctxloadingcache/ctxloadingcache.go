package ctxloadingcache

import (
	"context"

	"github.com/grailbio/base/sync/loadingcache"
)

type contextKeyType struct{}

var contextKey contextKeyType

// With returns a child context which, when passed to Value, gets and sets in m.
// Callers now control the lifetime of the returned context's cache and can clear it.
func With(ctx context.Context, m *loadingcache.Map) context.Context {
	return context.WithValue(ctx, contextKey, m)
}

// Value retrieves a *loadingcache.Value that's linked to a cache, if ctx was returned by With.
// If ctx didn't come from an earlier With call, there's no linked cache, so caching will be
// disabled by returning nil (which callers don't need to check, because nil is usable).
func Value(ctx context.Context, key interface{}) *loadingcache.Value {
	var m *loadingcache.Map
	if v := ctx.Value(contextKey); v != nil {
		m = v.(*loadingcache.Map)
	}
	return m.GetOrCreate(key)
}
