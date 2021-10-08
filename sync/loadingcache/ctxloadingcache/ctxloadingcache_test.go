package ctxloadingcache_test

import (
	"context"
	"testing"

	"github.com/grailbio/base/sync/loadingcache"
	"github.com/grailbio/base/sync/loadingcache/ctxloadingcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests check same-ness of *Value and rely on Value's unit testing for complete coverage.
// Theoretically an implementation could be correct but not preserve same-ness; if we change
// our implementation to do that, we can update these tests.

func TestBasic(t *testing.T) {
	var cache loadingcache.Map
	ctx := ctxloadingcache.With(context.Background(), &cache)
	var i1 int
	require.NoError(t, ctxloadingcache.Value(ctx, "key").
		GetOrLoad(ctx, &i1, func(ctx context.Context, opts *loadingcache.LoadOpts) error {
			i1 = 1
			opts.CacheForever()
			return nil
		}))
	assert.Equal(t, 1, i1)
	var i2 int
	require.NoError(t, ctxloadingcache.Value(ctx, "key").
		GetOrLoad(ctx, &i2, func(ctx context.Context, opts *loadingcache.LoadOpts) error {
			panic("computing i2")
		}))
	assert.Equal(t, 1, i2)
}

func TestKeys(t *testing.T) {
	var cache loadingcache.Map
	ctx := ctxloadingcache.With(context.Background(), &cache)

	type (
		testKeyA struct{}
		testKeyB struct{}
	)
	vA := ctxloadingcache.Value(ctx, testKeyA{})
	assert.NotSame(t, vA, ctxloadingcache.Value(ctx, testKeyB{}))
	assert.Same(t, vA, ctxloadingcache.Value(ctx, testKeyA{}))
}

func TestReuseKey(t *testing.T) {
	var cache loadingcache.Map
	ctx1 := ctxloadingcache.With(context.Background(), &cache)

	type testKey struct{}
	cache1 := ctxloadingcache.Value(ctx1, testKey{})

	ctx2 := context.WithValue(ctx1, testKey{}, "not a cache")
	assert.Same(t, cache1, ctxloadingcache.Value(ctx2, testKey{}))
}

func TestDeleteAll(t *testing.T) {
	var cache loadingcache.Map
	ctx1 := ctxloadingcache.With(context.Background(), &cache)
	ctx2, cancel2 := context.WithCancel(ctx1)
	defer cancel2()

	type (
		testKeyA struct{}
		testKeyB struct{}
	)
	vA := ctxloadingcache.Value(ctx1, testKeyA{})
	vB := ctxloadingcache.Value(ctx1, testKeyB{})
	assert.NotSame(t, vA, vB)
	assert.Same(t, vA, ctxloadingcache.Value(ctx1, testKeyA{}))
	assert.Same(t, vA, ctxloadingcache.Value(ctx2, testKeyA{}))
	assert.Same(t, vB, ctxloadingcache.Value(ctx2, testKeyB{}))

	cache.DeleteAll()

	assert.NotSame(t, vA, ctxloadingcache.Value(ctx1, testKeyA{}))
	assert.NotSame(t, vA, ctxloadingcache.Value(ctx2, testKeyA{}))
	assert.NotSame(t, vB, ctxloadingcache.Value(ctx2, testKeyB{}))
}
