// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package retry contains utilities for implementing retry logic.
package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/grailbio/base/errors"
)

// A Policy is an interface that abstracts retry policies. Typically
// users will not call methods directly on a Policy but rather use
// the package function retry.Wait.
type Policy interface {
	// Retry tells whether the a new retry should be attempted,
	// and after how long.
	Retry(retry int) (bool, time.Duration)
}

// Wait queries the provided policy at the provided retry number and
// sleeps until the next try should be attempted. Wait returns an
// error if the policy prohibits further tries or if the context was
// canceled, or if its deadline would run out while waiting for the
// next try.
func Wait(ctx context.Context, policy Policy, retry int) error {
	keepgoing, wait := policy.Retry(retry)
	if !keepgoing {
		return errors.E(errors.TooManyTries, fmt.Sprintf("gave up after %d tries", retry))
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < wait {
		return errors.E(errors.Timeout, "ran out of time while waiting for retry")
	}
	select {
	case <-time.After(wait):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type backoff struct {
	factor       float64
	initial, max time.Duration
}

// maxInt64Convertible is the maximum float64 that can be converted to an int64
// accurately. We use this to prevent overflow when computing the exponential
// backoff, which we compute with float64s. It is important that we push it
// through float64 then int64 so that we get compilation error if we use a
// value that cannot be represented as an int64. This value was produced with:
//   math.Nextafter(float64(math.MaxInt64), 0)
const maxInt64Convertible = int64(float64(9223372036854774784))

// MaxBackoffMax is the maximum value that can be passed as max to Backoff.
const MaxBackoffMax = time.Duration(maxInt64Convertible)

// Backoff returns a Policy that initially waits for the amount of
// time specified by parameter initial; on each try this value is
// multiplied by the provided factor, up to the max duration.
func Backoff(initial, max time.Duration, factor float64) Policy {
	if max > MaxBackoffMax {
		panic("max > MaxBackoffMax")
	}
	return &backoff{
		initial: initial,
		max:     max,
		factor:  factor,
	}
}

func (b *backoff) Retry(retries int) (bool, time.Duration) {
	if retries < 0 {
		panic("retries < 0")
	}
	nsfloat64 := float64(b.initial) * math.Pow(b.factor, float64(retries))
	nsfloat64 = math.Min(nsfloat64, float64(b.max))
	return true, time.Duration(int64(nsfloat64))
}

type jitter struct {
	policy Policy
	// frac is the fraction of the wait time to "jitter".
	// Eg: if frac is 0.2, the policy will retain 80% of the wait time
	// and jitter the remaining 20%
	frac float64
}

// Jitter returns a policy that jitters 'frac' fraction of the wait times
// returned  by the provided policy.  For example, setting frac to 1.0 and 0.5
// will implement "full jitter" and "equal jitter" approaches respectively.
// These approaches are describer here:
//
func Jitter(policy Policy, frac float64) Policy {
	return &jitter{policy, frac}
}

func (b *jitter) Retry(retries int) (bool, time.Duration) {
	ok, wait := b.policy.Retry(retries)
	if wait > 0 {
		prop := time.Duration(b.frac * float64(wait))
		wait = wait - prop + time.Duration(rand.Int63n(prop.Nanoseconds()))
	}
	return ok, wait
}

type maxtries struct {
	policy Policy
	max    int
}

// MaxTries returns a policy that enforces a maximum number of
// attempts. The provided policy is invoked when the current number
// of tries is within the permissible limit. If policy is nil, the
// returned policy will permit an immediate retry when the number of
// tries is within the allowable limits.
func MaxTries(policy Policy, n int) Policy {
	if n < 1 {
		panic("retry.MaxTries: n < 1")
	}
	return &maxtries{policy, n - 1}
}

func (m *maxtries) Retry(retries int) (bool, time.Duration) {
	if retries > m.max {
		return false, time.Duration(0)
	}
	if m.policy != nil {
		return m.policy.Retry(retries)
	}
	return true, time.Duration(0)
}
