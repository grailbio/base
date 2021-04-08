// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package retry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grailbio/base/errors"
)

func TestBackoff(t *testing.T) {
	policy := Backoff(time.Second, 10*time.Second, 2)
	expect := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		10 * time.Second,
		10 * time.Second,
	}
	for retries, wait := range expect {
		keepgoing, dur := policy.Retry(retries)
		if !keepgoing {
			t.Fatal("!keepgoing")
		}
		if got, want := dur, wait; got != want {
			t.Errorf("retry %d: got %v, want %v", retries, got, want)
		}
	}
}

// TestBackoffOverflow tests the behavior of exponential backoff for large
// numbers of retries.
func TestBackoffOverflow(t *testing.T) {
	policy := Backoff(time.Second, 10*time.Second, 2)
	expect := []time.Duration{
		10 * time.Second,
		10 * time.Second,
		10 * time.Second,
		10 * time.Second,
	}
	for retries, wait := range expect {
		// Use a large number of retries that might overflow exponential
		// calculations.
		keepgoing, dur := policy.Retry(1000 + retries)
		if !keepgoing {
			t.Fatal("!keepgoing")
		}
		if got, want := dur, wait; got != want {
			t.Errorf("retry %d: got %v, want %v", retries, got, want)
		}
	}
}

func TestBackoffWithFullJitter(t *testing.T) {
	policy := Jitter(Backoff(time.Second, 10*time.Second, 2), 1.0)
	checkWithin := func(t *testing.T, wantMin, wantMax, got time.Duration) {
		if got < wantMin || got > wantMax {
			t.Errorf("got %v, want within (%v, %v)", got, wantMin, wantMax)
		}
	}
	expect := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		10 * time.Second,
		10 * time.Second,
	}
	for retries, wait := range expect {
		keepgoing, dur := policy.Retry(retries)
		if !keepgoing {
			t.Fatal("!keepgoing")
		}
		checkWithin(t, 0, wait, dur)
	}
}

func TestBackoffWithEqualJitter(t *testing.T) {
	policy := Jitter(Backoff(time.Second, 10*time.Second, 2), 0.5)
	checkWithin := func(t *testing.T, wantMin, wantMax, got time.Duration) {
		if got < wantMin || got > wantMax {
			t.Errorf("got %v, want within (%v, %v)", got, wantMin, wantMax)
		}
	}
	expect := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		10 * time.Second,
		10 * time.Second,
	}
	for retries, wait := range expect {
		keepgoing, dur := policy.Retry(retries)
		if !keepgoing {
			t.Fatal("!keepgoing")
		}
		checkWithin(t, wait/2, wait, dur)
	}
}

func TestBackoffWithTimeout(t *testing.T) {
	policy := BackoffWithTimeout(time.Second, 10*time.Second, 2)
	expect := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
	}
	var retries = 0
	for _, wait := range expect {
		keepgoing, dur := policy.Retry(retries)
		if !keepgoing {
			t.Fatal("!keepgoing")
		}
		if got, want := dur, wait; got != want {
			t.Errorf("retry %d: got %v, want %v", retries, got, want)
		}
		retries++
	}
	keepgoing, _ := policy.Retry(retries)
	if keepgoing {
		t.Errorf("keepgoing: got %v, want %v", keepgoing, false)
	}

}

func TestWaitCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	policy := Backoff(time.Hour, time.Hour, 1)
	cancel()
	if got, want := Wait(ctx, policy, 0), context.Canceled; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestWaitDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	policy := Backoff(time.Hour, time.Hour, 1)
	if got, want := Wait(ctx, policy, 0), errors.E(errors.Timeout); !errors.Match(want, got) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func testWrapperHelper(i int) (int, error) {
	if i == 0 {
		return 0, fmt.Errorf("This is an Error")
	}
	return 9999, nil
}

func testWrapperHelperLong(i int) (int, int, error) {
	if i == 0 {
		return 0, 0, fmt.Errorf("This is an Error")
	}
	return 1, 2, nil
}

func TestWaitForFn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	policy := Backoff(time.Hour, time.Hour, 1)
	cancel()

	output := WaitForFn(ctx, policy, testWrapperHelper, 0)
	require.EqualError(t, output[1].Interface().(error), "This is an Error")

	output = WaitForFn(ctx, policy, testWrapperHelper, 55)
	require.Equal(t, 9999, int(output[0].Int()))

	var err error
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("wrong number of input, expected: 1, actual: 3")
		}
	}()
	WaitForFn(ctx, policy, testWrapperHelper, 1, 2, 3)
	require.EqualError(t, err, "wrong number of input, expected: 1, actual: 3")
}

func TestWaitForFnLong(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	policy := Backoff(time.Hour, time.Hour, 1)
	cancel()

	output := WaitForFn(ctx, policy, testWrapperHelperLong, 0)
	require.EqualError(t, output[2].Interface().(error), "This is an Error")

	output = WaitForFn(ctx, policy, testWrapperHelperLong, 55)
	require.Equal(t, 1, int(output[0].Int()))
	require.Equal(t, 2, int(output[1].Int()))

}

func TestMaxRetries(t *testing.T) {
	retryImmediately := Backoff(0, 0, 0)

	type testArgs struct {
		retryPolicy Policy
		fn          func(*int) error
	}
	testCases := []struct {
		testName string
		args     testArgs
		expected int
	}{
		{
			testName: "function always fails",
			args: testArgs{
				retryPolicy: MaxRetries(retryImmediately, 1),
				fn: func(callCount *int) error {
					*callCount++

					return fmt.Errorf("always fail")
				},
			},
			expected: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			callCount := 0

			WaitForFn(context.Background(), tc.args.retryPolicy, tc.args.fn, &callCount)

			require.Equal(t, tc.expected, callCount)
		})
	}
}
