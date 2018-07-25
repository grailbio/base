// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package limiter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grailbio/base/sync/ctxsync"
	"golang.org/x/time/rate"
)

// BatchLimiter provides the ability to batch calls and apply a rate limit (on the batches).
// Users have to provide an implementation of BatchApi and a rate.Limiter.
// Thereafter callers can concurrently Do calls for each individual ID and the BatchLimiter will
// batch calls (whenever appropriate) while respecting the rate limit.
// Individual requests are serviced in the order of submission.
type BatchLimiter struct {
	api     BatchApi
	limiter *rate.Limiter
	wait    time.Duration

	mu sync.Mutex
	// pending is the list of pending ids in the order of submission
	pending []ID
	// results maps each submitted ID to its result.
	results map[ID]*Result
}

// BatchApi needs to be implemented in order to use BatchLimiter.
type BatchApi interface {
	// MaxPerBatch is the max number of ids to call per `Do` (zero implies no limit).
	MaxPerBatch() int

	// Do the batch call with the given map of IDs to Results.
	// The implementation must call Result.Set to provide the Value or Err (as applicable) for the every ID.
	// At the end of this call, if Result.Set was not called on the result of a particular ID,
	// the corresponding ID's `Do` call will get ErrNoResult.
	Do(map[ID]*Result)
}

// ID is the identifier of each call.
type ID interface{}

// Result is the result of an API call for a given id.
type Result struct {
	mu       sync.Mutex
	cond     *ctxsync.Cond
	id       ID
	value    interface{}
	err      error
	done     bool
	nWaiters int
}

// Set sets the result of a given id with the given value v and error err.
func (r *Result) Set(v interface{}, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done = true
	r.value = v
	r.err = err
	r.cond.Broadcast()
}

func (r *Result) doneC() <-chan struct{} {
	r.mu.Lock()
	return r.cond.Done()
}

// NewBatchLimiter returns a new BatchLimiter which will call the given batch API
// as per the limits set by the given rate limiter.
func NewBatchLimiter(api BatchApi, limiter *rate.Limiter) *BatchLimiter {
	eventsPerSecond := limiter.Limit()
	if eventsPerSecond == 0 {
		panic("limiter does not allow any events")
	}
	d := float64(time.Second) / float64(eventsPerSecond)
	wait := time.Duration(d)
	return &BatchLimiter{api: api, limiter: limiter, wait: wait, results: make(map[ID]*Result)}
}

var ErrNoResult = fmt.Errorf("no result")

// Do submits the given ID to the batch limiter and returns the result or an error.
// If the returned error is ErrNoResult, it indicates that the batch call did not produce any result for the given ID.
// Callers may then apply their own retry strategy if necessary.
// Do merges duplicate calls if the IDs are of a comparable type (and if the result is still pending)
// However, de-duplication is not guaranteed.
// Callers can avoid de-duplication by using a pointer type instead.
func (l *BatchLimiter) Do(ctx context.Context, id ID) (interface{}, error) {
	var t *time.Timer
	defer func() {
		if t != nil {
			t.Stop()
		}
	}()
	r := l.register(id)
	defer l.unregister(r)
	for {
		if done, v, err := l.get(r); done {
			return v, err
		}
		if l.limiter.Allow() {
			m := l.claim()
			if len(m) > 0 {
				l.api.Do(m)
				l.update(m)
				continue
			}
		}
		// Wait half the interval to increase chances of making the next call as early as possible.
		d := l.wait / 2
		if t == nil {
			t = time.NewTimer(d)
		} else {
			t.Reset(d)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.doneC():
		case <-t.C:
		}
	}
}

// register registers the given id.
func (l *BatchLimiter) register(id ID) *Result {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.results[id]; !ok {
		l.pending = append(l.pending, id)
		r := &Result{id: id}
		r.cond = ctxsync.NewCond(&r.mu)
		l.results[id] = r
	}
	r := l.results[id]
	r.mu.Lock()
	r.nWaiters += 1
	r.mu.Unlock()
	return r
}

// unregister indicates that the calling goroutine is no longer interested in the given result.
func (l *BatchLimiter) unregister(r *Result) {
	var remove bool
	r.mu.Lock()
	r.nWaiters -= 1
	remove = r.nWaiters == 0
	r.mu.Unlock()
	if remove {
		l.mu.Lock()
		delete(l.results, r.id)
		l.mu.Unlock()
	}
}

// get returns whether the result is done and the value and error.
func (l *BatchLimiter) get(r *Result) (bool, interface{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done, r.value, r.err
}

// update updates the internal results using the given ones.
// update also sets ErrNoResult as the error result for IDs for which `Result.Set` was not called.
func (l *BatchLimiter) update(results map[ID]*Result) {
	for _, r := range results {
		r.mu.Lock()
		if !r.done {
			r.done, r.err = true, ErrNoResult
		}
		r.mu.Unlock()
	}
}

// claim claims pending ids and returns a mapping of those ids to their results.
func (l *BatchLimiter) claim() map[ID]*Result {
	l.mu.Lock()
	defer l.mu.Unlock()
	max := l.api.MaxPerBatch()
	if max == 0 {
		max = len(l.pending)
	}
	claimed := make(map[ID]*Result)
	i := 0
	for ; i < len(l.pending) && len(claimed) < max; i++ {
		id := l.pending[i]
		r := l.results[id]
		if r == nil {
			continue
		}
		r.mu.Lock()
		if !r.done {
			claimed[id] = r
		}
		r.mu.Unlock()
	}
	// Remove the claimed ids from the pending list.
	l.pending = l.pending[i:]
	return claimed
}
