package loadingcache

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"runtime/debug"
	"sync"
	"time"

	"github.com/grailbio/base/must"
	"github.com/grailbio/base/sync/ctxsync"
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
		mu   sync.Mutex
		cond *ctxsync.Cond
		// now is used for faking time in tests.
		now func() time.Time
		// expiresAt is the time of expiration (according to now) when in state-loaded.
		// expiresAt.IsZero() means no expiration (infinite caching).
		// In other states, expiresAt is meaningless.
		expiresAt time.Time
		// 3-state machine. See lockedEnterState*().
		inProgress bool
		dataPtr    reflect.Value
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

func (v *Value) lockedEnterStateEmpty() {
	v.expiresAt = time.Time{}
	v.inProgress = false
	v.dataPtr = reflect.Value{}
}

func (v *Value) lockedEnterStateLoading() {
	v.expiresAt = time.Time{}
	v.inProgress = true
	v.dataPtr = reflect.Value{}
}

func (v *Value) lockedEnterStateLoaded(dataPtr reflect.Value, expiresAt time.Time) {
	v.expiresAt = expiresAt
	v.inProgress = false
	v.dataPtr = dataPtr
}

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

	v.mu.Lock()
	defer v.mu.Unlock()

	// Initialize on first use.
	if v.now == nil {
		v.now = time.Now
	}
	if v.cond == nil {
		v.cond = ctxsync.NewCond(&v.mu)
	}

	for {
		switch {
		// State empty.
		case !v.inProgress && !v.dataPtr.IsValid():
			v.lockedEnterStateLoading()
			v.mu.Unlock()
			var opts LoadOpts
			err := runNoPanic(func() (err error) { return load(ctx, &opts) })
			v.mu.Lock()
			if err == nil && opts.validFor != 0 {
				var expiresAt time.Time
				if opts.validFor > 0 {
					expiresAt = v.now().Add(opts.validFor)
				}
				v.lockedEnterStateLoaded(ptrVal, expiresAt)
			} else {
				v.lockedEnterStateEmpty()
			}
			v.cond.Broadcast()
			return err

		// State loading.
		case v.inProgress && !v.dataPtr.IsValid():
			// Wait for a new state.
			if err := v.cond.Wait(ctx); err != nil {
				return err
			}

		// State loaded.
		case !v.inProgress && v.dataPtr.IsValid():
			if v.expiresAt.IsZero() || v.now().Before(v.expiresAt) {
				ptrVal.Elem().Set(v.dataPtr.Elem())
				return nil
			}
			// Cache is stale.
			v.lockedEnterStateEmpty()

		default:
			log.Panicf("invalid cache state: %v, %v, %v", v.expiresAt, v.inProgress, v.dataPtr)
		}
	}
}

func runNoPanic(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("cache: recovered panic: %v, stack:\n%v", r, string(debug.Stack()))
		}
	}()
	return f()
}

// setClock is for testing.
func (v *Value) setClock(now func() time.Time) {
	if v == nil {
		return
	}
	v.now = now
}

func (o *LoadOpts) CacheFor(d time.Duration) { o.validFor = d }
func (o *LoadOpts) CacheForever()            { o.validFor = -1 }
