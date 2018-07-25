package loadingcache

import (
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"time"

	"github.com/grailbio/base/must"
)

type (
	// Value manages the loading (calculation) and storing of a cache value. It's designed for use
	// cases where loading is slow. Concurrency is well-supported:
	//  1. Only one load is in progress at a time, even if concurrent callers request the value.
	//  2. Cancellation is respected for loading: a caller's load function is invoked with their
	//     context. If it respects cancellation and returns an error immediately, the caller's
	//     GetOrLoad does, too.
	//  3. Cancellation is respected for waiting: if a caller's context is canceled while they're
	//     waiting for another in-progress load (not their own), the caller's GetOrLoad returns
	//     immediately with the cancellation error.
	// Simpler mechanisms (like just locking a sync.Mutex when starting computation) don't achieve
	// all of these (in the mutex example, cancellation is not respected while waiting on Lock()).
	//
	// The original use case was reading rarely-changing data via RPC while letting users
	// cancel the operation (Ctrl-C in their terminal). Very different uses (very fast computations
	// or extremely high concurrency) may not work as well; they're at least not tested.
	// Memory overhead may be quite large if small values are cached.
	//
	// Value{} is ready to use. (*Value)(nil) is valid and just never caches or shares a result
	// (every get loads). Value must not be copied.
	//
	// Time-based expiration is optional. See LoadFunc and LoadOpts.
	Value struct {
		// init supports at-most-once initialization of subsequent fields.
		init sync.Once
		// c is both a semaphore (limit 1) and storage for cache state.
		c chan state
		// now is used for faking time in tests.
		now func() time.Time
	}
	state struct {
		// dataPtr is non-zero if there's a previously-computed value (which may be expired).
		dataPtr reflect.Value
		// expiresAt is the time of expiration (according to now) when dataPtr is non-zero.
		// expiresAt.IsZero() means no expiration (infinite caching).
		expiresAt time.Time
	}
	// LoadFunc computes a value. It should respect cancellation (return with cancellation error).
	LoadFunc func(context.Context, *LoadOpts) error
	// LoadOpts configures how long a LoadFunc result should be cached.
	// Cache settings overwrite each other; last write wins. Default is don't cache at all.
	// Callers should synchronize their calls themselves if using multiple goroutines (this is
	// not expected).
	LoadOpts struct {
		// validFor is cache time if > 0, disables cache if == 0, infinite cache time if < 0.
		validFor time.Duration
	}
)

// GetOrLoad either copies a cached value to dataPtr or runs load and then copies dataPtr's value
// into the cache. A properly-written load writes dataPtr's value. Example:
//
// 	var result string
// 	err := value.GetOrLoad(ctx, &result, func(ctx context.Context, opts *loadingcache.LoadOpts) error {
// 		var err error
// 		result, err = doExpensiveThing(ctx)
// 		opts.CacheFor(time.Hour)
// 		return err
// 	})
//
// dataPtr must be a pointer to a copyable value (slice, int, struct without Mutex, etc.).
//
// Value does not cache errors. Consider caching a value containing an error, like
// struct{result int; err error} if desired.
func (v *Value) GetOrLoad(ctx context.Context, dataPtr interface{}, load LoadFunc) error {
	ptrVal := reflect.ValueOf(dataPtr)
	must.True(ptrVal.Kind() == reflect.Ptr, "%v", dataPtr)
	// TODO: Check copyable?

	if v == nil {
		return runNoPanic(func() error {
			var opts LoadOpts
			return load(ctx, &opts)
		})
	}

	v.init.Do(func() {
		if v.c == nil {
			v.c = make(chan state, 1)
			v.c <- state{}
		}
		if v.now == nil {
			v.now = time.Now
		}
	})

	var state state
	select {
	case <-ctx.Done():
		return ctx.Err()
	case state = <-v.c:
	}
	defer func() { v.c <- state }()

	if state.dataPtr.IsValid() {
		if state.expiresAt.IsZero() || v.now().Before(state.expiresAt) {
			ptrVal.Elem().Set(state.dataPtr.Elem())
			return nil
		}
		state.dataPtr = reflect.Value{}
	}

	var opts LoadOpts
	// TODO: Consider calling load() directly rather than via runNoPanic().
	// A previous implementation needed to intercept panics to handle internal state correctly.
	// That's no longer true, so we can avoid tampering with callers' panic traces.
	err := runNoPanic(func() error { return load(ctx, &opts) })
	if err == nil && opts.validFor != 0 {
		state.dataPtr = ptrVal
		if opts.validFor > 0 {
			state.expiresAt = v.now().Add(opts.validFor)
		} else {
			state.expiresAt = time.Time{}
		}
	}
	return err
}

func runNoPanic(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("cache: recovered panic: %v, stack:\n%v", r, string(debug.Stack()))
		}
	}()
	return f()
}

// setClock is for testing. It must be called before any GetOrLoad and is not concurrency-safe.
func (v *Value) setClock(now func() time.Time) {
	if v == nil {
		return
	}
	v.now = now
}

func (o *LoadOpts) CacheFor(d time.Duration) { o.validFor = d }
func (o *LoadOpts) CacheForever()            { o.validFor = -1 }
