// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package retry

import (
	"context"
	"math"
	"testing"
	"time"

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
	// initial is carefully chosen so that initial*1.0001 produces a float64
	// that overflows int64 when cast but equals the float64 representation of
	// MaxInt64.
	initial := time.Duration(9222449791875588096)
	policy := Backoff(initial, math.MaxInt64, 1.0001)

	expect := []time.Duration{
		initial,
		math.MaxInt64,
		math.MaxInt64,
		math.MaxInt64,
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
