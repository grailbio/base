package loadingcache

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const recentUnixTimestamp = 1600000000 // 2020-09-13 12:26:40 +0000 UTC

func TestValueExpiration(t *testing.T) {
	var (
		ctx   = context.Background()
		v     Value
		clock fakeClock
	)
	v.setClock(clock.Now)

	clock.Set(time.Unix(recentUnixTimestamp, 0))
	var v1 int
	require.NoError(t, v.GetOrLoad(ctx, &v1, func(_ context.Context, opts *LoadOpts) error {
		clock.Add(2 * time.Hour)
		v1 = 1
		opts.CacheFor(time.Hour)
		return nil
	}))
	assert.Equal(t, 1, v1)

	clock.Add(5 * time.Minute)
	var v2 int
	require.NoError(t, v.GetOrLoad(ctx, &v2, loadFail))
	assert.Equal(t, 1, v1)
	assert.Equal(t, 1, v2)

	clock.Add(time.Hour)
	var v3 int
	require.NoError(t, v.GetOrLoad(ctx, &v3, func(_ context.Context, opts *LoadOpts) error {
		v3 = 3
		opts.CacheForever()
		return nil
	}))
	assert.Equal(t, 1, v1)
	assert.Equal(t, 1, v2)
	assert.Equal(t, 3, v3)

	clock.Add(10000 * time.Hour)
	var v4 int
	assert.NoError(t, v.GetOrLoad(ctx, &v4, loadFail))
	assert.Equal(t, 1, v1)
	assert.Equal(t, 1, v2)
	assert.Equal(t, 3, v3)
	assert.Equal(t, 3, v4)
}

func TestValueExpiration0(t *testing.T) {
	var (
		ctx   = context.Background()
		v     Value
		clock fakeClock
	)
	v.setClock(clock.Now)

	clock.Set(time.Unix(recentUnixTimestamp, 0))
	var v1 int
	require.NoError(t, v.GetOrLoad(ctx, &v1, func(_ context.Context, opts *LoadOpts) error {
		v1 = 1
		return nil
	}))
	assert.Equal(t, 1, v1)

	// Run v2 at the same time as v1. It should not get a cached result because v1's cache time was 0.
	var v2 int
	require.NoError(t, v.GetOrLoad(ctx, &v2, func(_ context.Context, opts *LoadOpts) error {
		v2 = 2
		opts.CacheFor(time.Hour)
		return nil
	}))
	assert.Equal(t, 1, v1)
	assert.Equal(t, 2, v2)
}

func TestValueNil(t *testing.T) {
	var (
		ctx   = context.Background()
		v     *Value
		clock fakeClock
	)
	v.setClock(clock.Now)

	clock.Set(time.Unix(recentUnixTimestamp, 0))
	var v1 int
	require.NoError(t, v.GetOrLoad(ctx, &v1, func(_ context.Context, opts *LoadOpts) error {
		clock.Add(2 * time.Hour)
		v1 = 1
		opts.CacheForever()
		return nil
	}))
	assert.Equal(t, 1, v1)

	var v2 int
	assert.Error(t, v.GetOrLoad(ctx, &v2, loadFail))
	assert.Equal(t, 1, v1)

	clock.Add(time.Hour)
	var v3 int
	require.NoError(t, v.GetOrLoad(ctx, &v3, func(_ context.Context, opts *LoadOpts) error {
		v3 = 3
		opts.CacheForever()
		return nil
	}))
	assert.Equal(t, 1, v1)
	assert.Equal(t, 3, v3)
}

func TestValueCancellation(t *testing.T) {
	var (
		v     Value
		clock fakeClock
	)
	v.setClock(clock.Now)
	clock.Set(time.Unix(recentUnixTimestamp, 0))
	const cacheDuration = time.Minute

	type participant struct {
		cancel context.CancelFunc
		// participant waits for these before proceeding.
		waitGet, waitLoad chan<- struct{}
		// participant returns these signals of its progress.
		loadStarted <-chan struct{}
		result      <-chan error
	}
	makeParticipant := func(dst *int, loaded int) participant {
		ctx, cancel := context.WithCancel(context.Background())
		var (
			waitGet     = make(chan struct{})
			waitLoad    = make(chan struct{})
			loadStarted = make(chan struct{})
			result      = make(chan error)
		)
		go func() {
			<-waitGet
			result <- v.GetOrLoad(ctx, dst, func(ctx context.Context, opts *LoadOpts) error {
				close(loadStarted)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-waitLoad:
					*dst = loaded
					opts.CacheFor(cacheDuration)
					return nil
				}
			})
		}()
		return participant{cancel, waitGet, waitLoad, loadStarted, result}
	}

	// Start participant 1 and wait for its cache load to start.
	var v1 int
	p1 := makeParticipant(&v1, 1)
	close(p1.waitGet)
	<-p1.loadStarted

	// Start participant 2, then cancel its context and wait for its error.
	var v2 int
	p2 := makeParticipant(&v2, 2)
	p2.waitGet <- struct{}{}
	p2.cancel()
	err2 := <-p2.result
	assert.True(t, errors.Is(errors.Canceled, err2), "got: %v", err2)

	// Start participant 3, then cancel participant 1 and wait for 3 to start loading.
	var v3 int
	p3 := makeParticipant(&v3, 3)
	p3.waitGet <- struct{}{}
	p1.cancel()
	<-p3.loadStarted
	err1 := <-p1.result
	assert.True(t, errors.Is(errors.Canceled, err1), "got: %v", err1)

	// Start participant 4 later (according to clock).
	var v4 int
	p4 := makeParticipant(&v4, 4)
	clock.Add(time.Second)
	p4.waitGet <- struct{}{}

	// Let participant 3 finish loading and wait for results.
	close(p3.waitLoad)
	require.NoError(t, <-p3.result)
	require.NoError(t, <-p4.result)
	assert.Equal(t, 3, v3)
	assert.Equal(t, 3, v4) // Got cached result.

	// Start participant 5 past cache time so it recomputes.
	var v5 int
	p5 := makeParticipant(&v5, 5)
	clock.Add(cacheDuration * 2)
	p5.waitGet <- struct{}{}
	close(p5.waitLoad)
	require.NoError(t, <-p5.result)
	assert.Equal(t, 3, v3)
	assert.Equal(t, 3, v4)
	assert.Equal(t, 5, v5)
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func loadFail(context.Context, *LoadOpts) error {
	panic("unexpected load")
}
