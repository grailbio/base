// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package retry contains utilities for implementing retry logic.
package retry

import (
	"context"
	"math"
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
		return errors.E(errors.TooManyTries, "retry policy ran out of tries")
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

// Backoff returns a Policy that nitially waits for the amount of
// time specified by parameter initial; on each try this value is
// multiplied by the provided factor, up to the max duration.
func Backoff(initial, max time.Duration, factor float64) Policy {
	return &backoff{
		initial: initial,
		max:     max,
		factor:  factor,
	}
}

func (b *backoff) Retry(retries int) (bool, time.Duration) {
	wait := time.Duration(float64(b.initial) * math.Pow(b.factor, float64(retries)))
	if wait > b.max {
		wait = b.max
	}
	return true, wait
}
