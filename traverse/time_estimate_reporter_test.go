// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package traverse

import (
	"testing"
	"time"
)

func TestTimeQueue(t *testing.T) {
	var queue timeQueue
	queue.init(5)

	t1 := time.Now()
	t2 := t1.Add(1)

	expectQueue(queue, []time.Time{}, t)
	queue.push(t1)
	expectQueue(queue, []time.Time{t1}, t)
	queue.push(t2)
	expectQueue(queue, []time.Time{t1, t2}, t)
	result := queue.peek()
	if result != t1 {
		t.Errorf("Expected Peek result: %v actual %v", t1, result)
	}
	expectQueue(queue, []time.Time{t1, t2}, t)
	result = queue.pop()
	if result != t1 {
		t.Errorf("Expected Pop resule: %v actual %v", t1, result)
	}
	expectQueue(queue, []time.Time{t2}, t)
}

func expectQueue(queue timeQueue, times []time.Time, t *testing.T) {
	if len(queue) != len(times) {
		t.Errorf("Expected queue: %v actual %v", times, queue)
	}
	for i, elem := range queue {
		if elem != times[i] {
			t.Errorf("Expected queue: %v actual %v", times, queue)
		}
	}
}

func TestBuildTimeLeftStr(t *testing.T) {
	currentTime := time.Now()

	tests := []struct {
		reporter TimeEstimateReporter
		expected string
	}{
		{
			reporter: TimeEstimateReporter{
				numWorkers:         1,
				numQueued:          10,
				numRunning:         0,
				numDone:            0,
				startTime:          currentTime,
				startTimes:         []time.Time{},
				cummulativeRuntime: time.Duration(0)},
			expected: "(0s left  0s avg)",
		},
		{
			reporter: TimeEstimateReporter{
				numWorkers:         1,
				numQueued:          9,
				numRunning:         1,
				numDone:            0,
				startTime:          currentTime.Add(-1 * time.Second),
				startTimes:         []time.Time{currentTime.Add(-1 * time.Second)},
				cummulativeRuntime: time.Duration(0),
			},
			expected: "(>9s left  1s avg)",
		},
		{
			reporter: TimeEstimateReporter{
				numWorkers:         1,
				numQueued:          9,
				numRunning:         0,
				numDone:            1,
				startTime:          currentTime.Add(-5 * time.Second),
				startTimes:         []time.Time{},
				cummulativeRuntime: time.Duration(5 * time.Second),
			},
			expected: "(~45s left  5s avg)",
		},
		{
			reporter: TimeEstimateReporter{
				numWorkers:         1,
				numQueued:          8,
				numRunning:         1,
				numDone:            1,
				startTime:          currentTime.Add(-10 * time.Second),
				startTimes:         []time.Time{currentTime.Add(-4 * time.Second)},
				cummulativeRuntime: time.Duration(5 * time.Second),
			},
			expected: "(~41s left  5s avg)",
		},
		{
			reporter: TimeEstimateReporter{
				numWorkers:         1,
				numQueued:          0,
				numRunning:         1,
				numDone:            9,
				startTime:          currentTime.Add(-45 * time.Second),
				startTimes:         []time.Time{currentTime.Add(-1 * time.Second)},
				cummulativeRuntime: time.Duration(9 * 5 * time.Second),
			},
			expected: "(~4s left  5s avg)",
		},
		{
			reporter: TimeEstimateReporter{
				numWorkers:         2,
				numQueued:          8,
				numRunning:         2,
				numDone:            0,
				startTime:          currentTime.Add(-2 * time.Second),
				startTimes:         []time.Time{currentTime.Add(-2 * time.Second), currentTime.Add(-1 * time.Second)},
				cummulativeRuntime: time.Duration(0),
			},
			expected: "(>9s left  2s avg)",
		},
		{
			reporter: TimeEstimateReporter{
				numWorkers:         2,
				numQueued:          6,
				numRunning:         2,
				numDone:            2,
				startTime:          currentTime.Add(-14 * time.Second),
				startTimes:         []time.Time{currentTime.Add(-4 * time.Second), currentTime.Add(-2 * time.Second)},
				cummulativeRuntime: time.Duration(2 * 5 * time.Second),
			},
			expected: "(~17s left  5s avg)",
		},
		{
			reporter: TimeEstimateReporter{
				numWorkers:         2,
				numQueued:          2,
				numRunning:         0,
				numDone:            8,
				startTime:          currentTime.Add(-45 * time.Second),
				startTimes:         []time.Time{},
				cummulativeRuntime: time.Duration(8 * 5 * time.Second),
			},
			expected: "(~5s left  5s avg)",
		},
		{ // Note even though we have 2 workers, only one can process the single queued job, so expected time left is 5s.
			reporter: TimeEstimateReporter{
				numWorkers:         2,
				numQueued:          1,
				numRunning:         0,
				numDone:            9,
				startTime:          currentTime.Add(-45 * time.Second),
				startTimes:         []time.Time{},
				cummulativeRuntime: time.Duration(9 * 5 * time.Second),
			},
			expected: "(~5s left  5s avg)",
		},
		{
			reporter: TimeEstimateReporter{
				numWorkers:         2,
				numQueued:          0,
				numRunning:         1,
				numDone:            9,
				startTime:          currentTime.Add(-48 * time.Second),
				startTimes:         []time.Time{currentTime.Add(-3 * time.Second)},
				cummulativeRuntime: time.Duration(9 * 5 * time.Second),
			},
			expected: "(~2s left  5s avg)",
		},
		{ // Last job is taking longer than average to run.
			reporter: TimeEstimateReporter{
				numWorkers:         2,
				numQueued:          0,
				numRunning:         1,
				numDone:            9,
				startTime:          currentTime.Add(-52 * time.Second),
				startTimes:         []time.Time{currentTime.Add(-7 * time.Second)},
				cummulativeRuntime: time.Duration(9 * 5 * time.Second),
			},
			expected: "(~0s left  5s avg)",
		},
	}

	for _, test := range tests {
		timeLeftStr := test.reporter.buildTimeLeftStr(currentTime)
		if timeLeftStr != test.expected {
			t.Errorf("Got time left string: %s, expected %s", timeLeftStr, test.expected)
		}
	}
}
