// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package traverse

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func recovered(f func()) (v interface{}) {
	defer func() { v = recover() }()
	f()
	return v
}

func TestTraverse(t *testing.T) {
	list := make([]int, 5)
	err := Each(5).Do(func(i int) error {
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
	err = Each(5).Do(func(i int) error {
		if i == 3 {
			return expectedErr
		}
		return nil
	})
	if got, want := err, expectedErr; got != want {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestPanic(t *testing.T) {
	expectedPanic := "panic in the disco!!"
	f := func() {
		Each(5).Do(func(i int) error {
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

func TestSharding(t *testing.T) {
	tests := []struct {
		n       int
		nshards int
	}{
		{
			n:       5,
			nshards: 5,
		},
		{
			n:       5,
			nshards: 10,
		},
		{
			n:       5,
			nshards: 2,
		},
		{
			n:       15,
			nshards: 3,
		},
	}

	for _, test := range tests {
		expectedList := make([]int, test.n)
		for i := range expectedList {
			expectedList[i] = i
		}

		list := make([]int, test.n)
		err := Each(test.n).Sharded(test.nshards).Do(func(i int) error {
			list[i] += i
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(list, expectedList) {
			t.Errorf("got %v, want %v", list, expectedList)
		}

		rangeList := make([]int, test.n)
		err = Each(test.n).Sharded(test.nshards).DoRange(func(start, end int) error {
			for i := start; i < end; i++ {
				rangeList[i] += i
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(rangeList, expectedList) {
			t.Errorf("DoRange failed: got %v, want %v", rangeList, expectedList)
		}

		// test error propagation
		expectedErr := errors.New("test error")
		err = Each(test.n).Sharded(test.nshards).Do(func(i int) error {
			if i == test.n/2 {
				return expectedErr
			}
			return nil
		})
		if got, want := err, expectedErr; got != want {
			t.Errorf("got %v want %v", got, want)
		}

		err = Each(test.n).Sharded(test.nshards).DoRange(func(start, end int) error {
			for i := start; i < end; i++ {
				if i == test.n/2 {
					return expectedErr
				}
			}
			return nil
		})
		if got, want := err, expectedErr; got != want {
			t.Errorf("got %v want %v", got, want)
		}
	}
}

type testStatus struct {
	queued, running, done int32
}

type testReporter struct {
	statusHistory []testStatus
}

func (reporter *testReporter) Report(queued, running, done int32) {
	reporter.statusHistory =
		append(reporter.statusHistory, testStatus{queued: queued, running: running, done: done})
}

func TestReportingSingleJob(t *testing.T) {
	reporter := testReporter{}

	Each(5).Limit(1).WithReporter(&reporter).Do(func(i int) error {
		return nil
	})

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
	reporter := testReporter{}

	numJobs := 50
	numConcurrent := 5

	Each(numJobs).Limit(numConcurrent).WithReporter(&reporter).Do(func(i int) error {
		return nil
	})

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
		err := Each(n).Do(func(i int) error {
			arr[i]++
			return nil
		})
		if err != nil {
			b.Error(err)
		}
	}
}

func benchmarkDo(n int, nshards int, b *testing.B) {
	arr := make([]int, n)
	for k := 0; k < b.N; k++ {
		err := Each(n).Sharded(nshards).Do(func(i int) error {
			arr[i]++
			return nil
		})
		if err != nil {
			b.Error(err)
		}
	}
}

func benchmarkDoRange(n int, nshards int, b *testing.B) {
	arr := make([]int, n)
	for k := 0; k < b.N; k++ {
		err := Each(n).Sharded(nshards).DoRange(func(start, end int) error {
			for i := start; i < end; i++ {
				arr[i]++
			}
			return nil
		})
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkDoShardSize1(b *testing.B) {
	benchmarkDo(1000, 1000, b)
}

func BenchmarkDoRangeShardSize1(b *testing.B) {
	benchmarkDoRange(1000, 1000, b)
}

func BenchmarkDoShardSize10(b *testing.B) {
	benchmarkDo(1000, 100, b)
}

func BenchmarkDoRangeShardSize10(b *testing.B) {
	benchmarkDoRange(1000, 100, b)
}

func BenchmarkDoShardSize100(b *testing.B) {
	benchmarkDo(1000, 10, b)
}

func BenchmarkDoRangeShardSize100(b *testing.B) {
	benchmarkDoRange(1000, 10, b)
}

func BenchmarkDoShardSize1000(b *testing.B) {
	benchmarkDo(1000, 1, b)
}

func BenchmarkDoRangeShardSize1000(b *testing.B) {
	benchmarkDoRange(1000, 1, b)
}
