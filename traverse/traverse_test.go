// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package traverse_test

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/grailbio/base/traverse"
)

func recovered(f func()) (v interface{}) {
	defer func() { v = recover() }()
	f()
	return v
}

func TestTraverse(t *testing.T) {
	list := make([]int, 5)
	err := traverse.Each(5, func(i int) error {
		list[i] += i
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := list, []int{0, 1, 2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	expectedErr := errors.New("test error")
	err = traverse.Each(5, func(i int) error {
		if i == 3 {
			return expectedErr
		}
		return nil
	})
	if got, want := err, expectedErr; got != want {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestRange(t *testing.T) {
	const N = 5000
	var (
		counts      = make([]int64, N)
		invocations int64
	)
	var tr traverse.T
	for i := 0; i < N; i++ {
		tr.Limit = rand.Intn(N*2) + 1
		err := tr.Range(N, func(start, end int) error {
			if start < 0 || end > N || end < start {
				return fmt.Errorf("invalid range [%d,%d)", start, end)
			}
			atomic.AddInt64(&invocations, 1)
			for i := start; i < end; i++ {
				atomic.AddInt64(&counts[i], 1)
			}
			return nil
		})
		if err != nil {
			t.Errorf("limit %d: %v", tr.Limit, err)
			continue
		}
		expect := int64(tr.Limit)
		if expect > N {
			expect = N
		}
		if got, want := invocations, expect; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		invocations = 0
		for i := range counts {
			if got, want := counts[i], int64(1); got != want {
				t.Errorf("counts[%d,%d]: got %v, want %v", i, tr.Limit, got, want)
			}
			counts[i] = 0
		}
	}
}

func TestPanic(t *testing.T) {
	expectedPanic := "panic in the disco!!"
	f := func() {
		traverse.Each(5, func(i int) error {
			if i == 3 {
				panic(expectedPanic)
			}
			return nil
		})
	}
	v := recovered(f)
	s, ok := v.(string)
	if !ok {
		t.Fatal("expected string")
	}
	if got, want := s, fmt.Sprintf("traverse child: %s", expectedPanic); !strings.HasPrefix(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

type testStatus struct {
	queued, running, done int32
}

type testReporter struct {
	mu                    sync.Mutex
	statusHistory         []testStatus
	queued, running, done int32
}

func (r *testReporter) Init(n int) {
	r.update(int32(n), 0, 0)
}

func (r *testReporter) Complete() {}

func (r *testReporter) Begin(i int) {
	r.update(-1, 1, 0)
}

func (r *testReporter) End(i int) {
	r.update(0, -1, 1)
}

func (r *testReporter) update(queued, running, done int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queued += queued
	r.running += running
	r.done += done
	r.statusHistory =
		append(r.statusHistory, testStatus{queued: r.queued, running: r.running, done: r.done})
}

func TestReportingSingleJob(t *testing.T) {
	reporter := new(testReporter)

	tr := traverse.T{Reporter: reporter}
	tr.Each(5, func(i int) error { return nil })

	expectedStatuses := []testStatus{
		testStatus{queued: 5, running: 0, done: 0},
		testStatus{queued: 4, running: 1, done: 0},
		testStatus{queued: 4, running: 0, done: 1},
		testStatus{queued: 3, running: 1, done: 1},
		testStatus{queued: 3, running: 0, done: 2},
		testStatus{queued: 2, running: 1, done: 2},
		testStatus{queued: 2, running: 0, done: 3},
		testStatus{queued: 1, running: 1, done: 3},
		testStatus{queued: 1, running: 0, done: 4},
		testStatus{queued: 0, running: 1, done: 4},
		testStatus{queued: 0, running: 0, done: 5},
	}

	for i, status := range reporter.statusHistory {
		if status != expectedStatuses[i] {
			t.Errorf("Expected status %v, got status %v, full log %v",
				expectedStatuses[i], status, reporter.statusHistory)
		}
	}
}

func TestReportingManyJobs(t *testing.T) {
	reporter := new(testReporter)

	numJobs := 50
	numConcurrent := 5

	tr := traverse.T{Limit: numConcurrent, Reporter: reporter}
	tr.Each(numJobs, func(i int) error { return nil })

	// first status should be all jobs queued
	if (reporter.statusHistory[0] != testStatus{queued: int32(numJobs), running: 0, done: 0}) {
		t.Errorf("First status should be all jobs queued, instead got %v", reporter.statusHistory[0])
	}

	// last status should be all jobs done
	numStatuses := len(reporter.statusHistory)
	if (reporter.statusHistory[numStatuses-1] != testStatus{queued: 0, running: 0, done: int32(numJobs)}) {
		t.Errorf("Last status should be all jobs done, instead got %v", reporter.statusHistory[numJobs-1])
	}

	for i, status := range reporter.statusHistory {
		if (status.queued + status.running + status.done) != int32(numJobs) {
			t.Errorf("Total number of jobs is not equal to numJobs = %d - status: %v", numJobs, status)
		}

		if status.queued < 0 || status.running < 0 || status.done < 0 {
			t.Errorf("Number of jobs can't be <0, status: %v", status)
		}

		if status.running > int32(numConcurrent) {
			t.Errorf("Can't have more than %d jobs running, status: %v", numConcurrent, status)
		}

		if i > 0 {
			previousStatus := reporter.statusHistory[i-1]

			if status == previousStatus {
				t.Errorf("Can't have the same status repeat - status: %v, previous status: %v",
					status, previousStatus)
			}

			if status.queued > previousStatus.queued {
				t.Errorf("Can't have queued jobs count increase - status: %v, prevoius status: %v",
					status, previousStatus)
			}

			if status.done < previousStatus.done {
				t.Errorf("Can't have done jobs count decrease - status: %v, previous status: %v",
					status, previousStatus)
			}
		}
	}
}

func BenchmarkDo(b *testing.B) {
	arr := make([]int, b.N)
	for n := 0; n < b.N; n++ {
		err := traverse.Each(n, func(i int) error {
			arr[i]++
			return nil
		})
		if err != nil {
			b.Error(err)
		}
	}
}

func benchmarkDo(n int, b *testing.B) {
	arr := make([]int, n)
	for k := 0; k < b.N; k++ {
		err := traverse.Each(n, func(i int) error {
			arr[i]++
			return nil
		})
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkDo1000(b *testing.B) {
	benchmarkDo(1000, b)
}

func BenchmarkDo10000(b *testing.B) {
	benchmarkDo(10000, b)
}
