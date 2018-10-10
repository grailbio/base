// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package traverse provides primitives for concurrent and parallel
// traversal of slices or user-defined collections.
package traverse

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/grailbio/base/errorreporter"
)

// A T is a traverser: it provides facilities for concurrently
// invoking functions that traverse collections of data.
type T struct {
	// Limit is the traverser's concurrency limit: there will be no more
	// than Limit concurrent invocations per traversal. A limit value of
	// zero (the default value) denotes no limit.
	Limit int
	// Reporter receives status reports for each traversal. It is
	// intended for users who wish to monitor the progress of large
	// traversal jobs.
	Reporter Reporter
}

// Limit returns a traverser with limit n.
func Limit(n int) T {
	return T{Limit: n}
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
// is returned. Each also propagates panics from underlying
// invocations to the caller.
func (t T) Each(n int, fn func(i int) error) error {
	max := n
	if t.Limit > 0 && t.Limit < max {
		max = t.Limit
	}
	var (
		wg     sync.WaitGroup
		errors errorreporter.T
		next   int64 = -1
	)
	wg.Add(max)
	if t.Reporter != nil {
		t.Reporter.Init(n)
		defer t.Reporter.Complete()
	}
	for i := 0; i < max; i++ {
		go func() {
			for {
				which := int(atomic.AddInt64(&next, 1))
				if which >= n || errors.Err() != nil {
					break
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
		}()
	}
	wg.Wait()
	err := errors.Err()
	if err == nil {
		return nil
	}
	// Propagate panics.
	if err, ok := err.(panicErr); ok {
		panic(fmt.Sprintf("traverse child: %s\n%s", err.v, string(err.stack)))
	}
	return err
}

// Range performs ranged traversal on fn: n is split is split into
// contiguous ranges, and fn is invoked for each range. The range
// sizes are determined by the traverser's concurrency limits. Range
// allows the caller to amortize function call costs, and is
// typically used when limit is small and n is large, for example on
// parallel traversal over large collections, where each item's
// processing time is comparatively small.
func (t T) Range(n int, fn func(start, end int) error) error {
	m := n
	if t.Limit > 0 && t.Limit < n {
		m = t.Limit
	}
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
