// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package traverse provides primitives for concurrent and parallel
// traversal of slices or user-defined collections.
package traverse

import (
	"fmt"
	"log"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/grailbio/base/errors"
)

const cachelineSize = 64

// A T is a traverser: it provides facilities for concurrently
// invoking functions that traverse collections of data.
type T struct {
	// Limit is the traverser's concurrency limit: there will be no more
	// than Limit concurrent invocations per traversal. A limit value of
	// zero (the default value) denotes no limit.
	Limit int
	// Sequential indicates that early indexes should be handled before later
	// ones.  E.g. if there are 40000 tasks and Limit == 40, the initial
	// assignment is usually
	//   worker 0 <- tasks 0-999
	//   worker 1 <- tasks 1000-1999
	//   ...
	//   worker 39 <- tasks 39000-39999
	// but when Sequential == true, only tasks 0-39 are initially assigned, then
	// task 40 goes to the first worker to finish, etc.
	// Note that this increases synchronization overhead.  It should not be used
	// with e.g. > 1 billion tiny tasks; in that scenario, the caller should
	// organize such tasks into e.g. 10000-task chunks and perform a
	// sequential-traverse on the chunks.
	// This scheduling algorithm does perform well when tasks are sorted in order
	// of decreasing size.
	Sequential bool
	// Reporter receives status reports for each traversal. It is
	// intended for users who wish to monitor the progress of large
	// traversal jobs.
	Reporter Reporter
}

// Limit returns a traverser with limit n.
func Limit(n int) T {
	if n <= 0 {
		log.Panicf("traverse.Limit: invalid limit: %d", n)
	}
	return T{Limit: n}
}

// LimitSequential returns a sequential traverser with limit n.
func LimitSequential(n int) T {
	if n <= 0 {
		log.Panicf("traverse.LimitSequential: invalid limit: %d", n)
	}
	return T{Limit: n, Sequential: true}
}

// Parallel is the default traverser for parallel traversal, intended
// CPU-intensive parallel computing. Parallel limits the number of
// concurrent invocations to a small multiple of the runtime's
// available processors.
var Parallel = T{Limit: 2 * runtime.GOMAXPROCS(0)}

// Each performs a traversal on fn. Specifically, Each invokes fn(i)
// for 0 <= i < n, managing concurrency and error propagation. Each
// returns when the all invocations have completed, or after the
// first invocation fails, in which case the first invocation error
// is returned. Each also propagates panics from underlying invocations
// to the caller. Note that if a function panics and doesn't release
// shared resources that fn might need in a traverse child, this could
// lead to deadlock.
func (t T) Each(n int, fn func(i int) error) error {
	if t.Reporter != nil {
		t.Reporter.Init(n)
		defer t.Reporter.Complete()
	}
	var err error
	if t.Limit == 1 || n == 1 {
		err = t.eachSerial(n, fn)
	} else if t.Limit == 0 || t.Limit >= n {
		err = t.each(n, fn)
	} else if t.Sequential {
		err = t.eachSequential(n, fn)
	} else {
		err = t.eachLimit(n, fn)
	}
	if err == nil {
		return nil
	}
	// Propagate panics.
	if err, ok := err.(panicErr); ok {
		panic(fmt.Sprintf("traverse child: %v\n%s", err.v, string(err.stack)))
	}
	return err
}

func (t T) each(n int, fn func(i int) error) error {
	var (
		errors errors.Once
		wg     sync.WaitGroup
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			if t.Reporter != nil {
				t.Reporter.Begin(i)
			}
			if err := apply(fn, i); err != nil {
				errors.Set(err)
			}
			if t.Reporter != nil {
				t.Reporter.End(i)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	return errors.Err()
}

// eachSerial runs on the local thread using a conventional for loop.
// all invocations will be run in numerical order.
func (t T) eachSerial(n int, fn func(i int) error) error {
	for i := 0; i < n; i++ {
		if t.Reporter != nil {
			t.Reporter.Begin(i)
		}
		if err := apply(fn, i); err != nil {
			return err
		}
		if t.Reporter != nil {
			t.Reporter.End(i)
		}
	}
	return nil
}

// eachSequential performs a concurrent run where tasks are assigned in strict
// numerical order.  Unlike eachLimit(), it can be used when the traversal must
// be done sequentially.
func (t T) eachSequential(n int, fn func(i int) error) error {
	var (
		errors     errors.Once
		wg         sync.WaitGroup
		syncStruct struct {
			_ [cachelineSize - 8]byte // cache padding
			N int64
			_ [cachelineSize - 8]byte // cache padding
		}
	)
	syncStruct.N = -1
	wg.Add(t.Limit)
	for i := 0; i < t.Limit; i++ {
		go func() {
			for errors.Err() == nil {
				idx := int(atomic.AddInt64(&syncStruct.N, 1))
				if idx >= n {
					break
				}
				if t.Reporter != nil {
					t.Reporter.Begin(idx)
				}
				if err := apply(fn, idx); err != nil {
					errors.Set(err)
				}
				if t.Reporter != nil {
					t.Reporter.End(idx)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return errors.Err()
}

// eachLimit performs a concurrent run where tasks can be assigned in any
// order.
func (t T) eachLimit(n int, fn func(i int) error) error {
	var (
		errors errors.Once
		wg     sync.WaitGroup
		next   = make([]struct {
			N int64
			_ [cachelineSize - 8]byte // cache padding
		}, t.Limit)
		size = (n + t.Limit - 1) / t.Limit
	)
	wg.Add(t.Limit)
	for i := 0; i < t.Limit; i++ {
		go func(w int) {
			orig := w
			for errors.Err() == nil {
				// Each worker traverses contiguous segments since there is
				// often usable data locality associated with index locality.
				idx := int(atomic.AddInt64(&next[w].N, 1) - 1)
				which := w*size + idx
				if idx >= size || which >= n {
					w = (w + 1) % t.Limit
					if w == orig {
						break
					}
					continue
				}
				if t.Reporter != nil {
					t.Reporter.Begin(which)
				}
				if err := apply(fn, which); err != nil {
					errors.Set(err)
				}
				if t.Reporter != nil {
					t.Reporter.End(which)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	return errors.Err()
}

// Range performs ranged traversal on fn: n is split into
// contiguous ranges, and fn is invoked for each range. The range
// sizes are determined by the traverser's concurrency limits. Range
// allows the caller to amortize function call costs, and is
// typically used when limit is small and n is large, for example on
// parallel traversal over large collections, where each item's
// processing time is comparatively small.
func (t T) Range(n int, fn func(start, end int) error) error {
	if t.Sequential {
		// interface for this should take a chunk size.
		log.Panicf("traverse.Range: sequential traversal unsupported")
	}
	m := n
	if t.Limit > 0 && t.Limit < n {
		m = t.Limit
	}
	// TODO: consider splitting ranges into smaller chunks so that can
	// take better advantage of the load balancing underneath.
	return t.Each(m, func(i int) error {
		var (
			size  = float64(n) / float64(m)
			start = int(float64(i) * size)
			end   = int(float64(i+1) * size)
		)
		if start >= n {
			return nil
		}
		if i == m-1 {
			end = n
		}
		return fn(start, end)
	})
}

var defaultT = T{}

// Each performs concurrent traversal over n elements. It is a
// shorthand for (T{}).Each.
func Each(n int, fn func(i int) error) error {
	return defaultT.Each(n, fn)
}

// CPU calls the function fn for each available system CPU. CPU
// returns when all calls have completed or on first error.
func CPU(fn func() error) error {
	return Each(runtime.NumCPU(), func(int) error { return fn() })
}

func apply(fn func(i int) error, i int) (err error) {
	defer func() {
		if perr := recover(); perr != nil {
			err = panicErr{perr, debug.Stack()}
		}
	}()
	return fn(i)
}

type panicErr struct {
	v     interface{}
	stack []byte
}

func (p panicErr) Error() string { return fmt.Sprint(p.v) }
