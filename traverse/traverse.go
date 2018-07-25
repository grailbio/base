// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package traverse provides facilities for concurrent and parallel slice traversal.
package traverse

import (
	"fmt"
	"github.com/grailbio/base/errorreporter"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

type panicErr struct {
	v     interface{}
	stack []byte
}

func (p panicErr) Error() string { return fmt.Sprint(p.v) }

// Traverse is a traversal of a given length. Traverse instances
// should be instantiated with Each and Parallel.
type Traverse struct {
	n, maxConcurrent, nshards int
	debugStatus               *status
}

// Each creates a new traversal of length n appropriate for
// concurrent traversal.
func Each(n int) Traverse {
	return Traverse{n, n, 0, nil}
}

// Parallel creates a new traversal of length n appropriate for
// parallel traversal.
func Parallel(n int) Traverse {
	return Each(n).Limit(runtime.NumCPU())
}

// Limit limits the concurrency of the traversal to maxConcurrent.
func (t Traverse) Limit(maxConcurrent int) Traverse {
	t.maxConcurrent = maxConcurrent
	return t
}

// Sharded sets the number of shards we want to use for the traverse
// (for traverse of large number of elements where processing each element
// is very fast, it will be more efficient to shard the processing,
// rather than use a separate goroutine for each element).
// If not set, by default the number of shards is equal to the number
// of elements to traverse (shard size 1).
// When using a Reporter, each shard will be reported as a single job.
func (t Traverse) Sharded(nshards int) Traverse {
	t.nshards = nshards
	return t
}

// WithReporter will use the given reporter to report the progress on the jobs.
// Ex. traverse.Each(9).WithReporter(traverse.DefaultReporter{Name: "Processing:"}).Do(func(i int) error { ...
func (t Traverse) WithReporter(reporter Reporter) Traverse {
	t.debugStatus = &status{&sync.Mutex{}, reporter, 0, 0, 0}
	return t
}

// Do performs a traversal, invoking function op for each index, 0 <=
// i < t.n. Do returns the first error returned by any invoked op, or
// nil when all ops succeed. Traversal is terminated early on error.
// Panics are recovered in ops and propagated to the calling
// goroutine, printing the original stack trace. Do guarantees that,
// after it returns, no more ops will be invoked.
func (t Traverse) Do(op func(i int) error) (err error) {
	return t.DoRange(func(start, end int) error {
		for i := start; i < end && err == nil; i++ {
			err = op(i)
		}
		return err
	})
}

// DoRange is similar to Do above, except it accepts a function that runs
// over a block of indices [start, end).  This can be more efficient if
// running sharded traverse on an input where the operation for each index
// if very simple/fast.  For example, to add 1 to each element of a []int
// traverse.Each(len(slice)).Limit(10).Sharded(10).DoRange(func(start, end int) error {
//     for i := start; i < end; i++ {
//         slice[i]++
//     }
//     return nil
// }
func (t Traverse) DoRange(op func(start, end int) error) error {
	if t.n == 0 {
		return nil
	}

	numShards := t.n
	shardSize := 1
	if t.nshards > 0 {
		numShards = min(t.nshards, t.n)
		shardSize = (t.n + t.nshards - 1) / t.nshards
	}

	if numShards < t.maxConcurrent {
		t.maxConcurrent = numShards
	}

	var errorReporter errorreporter.T
	apply := func(i int) (err error) {
		defer func() {
			if perr := recover(); perr != nil {
				err = panicErr{perr, debug.Stack()}
			}
		}()
		start := i * shardSize
		return op(start, min(start+shardSize, t.n))
	}
	var wg sync.WaitGroup
	wg.Add(t.maxConcurrent)
	t.debugStatus.queueJobs(int32(numShards))

	var x int64 = -1 // x is treated with atomic operations and accessed from multiple go routines
	for i := 0; i < t.maxConcurrent; i++ {
		go func() {
			defer wg.Done()
			for {
				i := int(atomic.AddInt64(&x, 1)) // the first iteration will return 0.
				if i >= numShards || errorReporter.Err() != nil {
					return
				}
				t.debugStatus.startJob()
				err := apply(i)
				t.debugStatus.finishJob()
				if err != nil {
					errorReporter.Set(err)
					return
				}
			}
		}()
	}

	wg.Wait()
	// read the first errors that may have occurred
	foundError := errorReporter.Err()
	if foundError != nil {
		if err, ok := foundError.(panicErr); ok {
			panic(fmt.Sprintf("traverse child: %s\n%s", err.v, string(err.stack)))
		}
		return foundError
	}
	return nil
}

// Reporter is the interface for reporting the progress on traverse jobs.
type Reporter interface {
	// Report is called every time the number of jobs queued, running, or done changes.
	Report(queued, running, done int32)
}

// DefaultReporter is a simple Reporter that prints to stderr the number of
// jobs queued, running, and done
type DefaultReporter struct {
	Name string
}

// Report prints the number of jobs currently queued, running, and done.
func (reporter DefaultReporter) Report(queued, running, done int32) {
	fmt.Fprintf(os.Stderr, "%s: (queued: %d -> running: %d -> done: %d) \r", reporter.Name, queued, running, done)
	if queued == 0 && running == 0 {
		fmt.Fprintf(os.Stderr, "\n")
	}
}

// status keeps track of how many jobs are queued, running, and done.
type status struct {
	mu       *sync.Mutex
	reporter Reporter
	queued   int32
	done     int32
	running  int32
}

func (s *status) queueJobs(numjobs int32) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.queued += numjobs
	s.reporter.Report(s.queued, s.running, s.done)
	s.mu.Unlock()
}

func (s *status) startJob() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.queued--
	s.running++
	s.reporter.Report(s.queued, s.running, s.done)
	s.mu.Unlock()
}

func (s *status) finishJob() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.running--
	s.done++
	s.reporter.Report(s.queued, s.running, s.done)
	s.mu.Unlock()
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
