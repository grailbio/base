// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package admit contains utilities for admission control.
package admit

import (
	"context"
	"expvar"
	"sync"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/base/sync/ctxsync"
)

var (
	admitLimit = expvar.NewMap("admit.limit")
	admitUsed  = expvar.NewMap("admit.used")
)

// Policy implements the low level details of an admission control
// policy. Users typically use a utility function such as admit.Do
// or admit.Retry.
type Policy interface {
	// Acquire acquires a number of tokens from the admission controller.
	// Returns on success, or if the context was canceled.
	// Acquire can also return with an error if the number of requested tokens
	// exceeds the upper limit of available tokens.
	Acquire(ctx context.Context, need int) error

	// Release a number of tokens to the admission controller,
	// reporting whether the request was within the capacity limits.
	Release(tokens int, ok bool)
}

// Do calls f after being admitted by the controller. f's bool return value is
// passed on to the underlying policy upon Release, and the error is simply
// returned back to the caller as a convenience.
// If policy is nil, then this will simply call f.
func Do(ctx context.Context, policy Policy, tokens int, f func() (bool, error)) error {
	if policy == nil {
		_, err := f()
		return err
	}
	if err := policy.Acquire(ctx, tokens); err != nil {
		return err
	}
	var (
		ok  bool
		err error
	)
	defer func() { policy.Release(tokens, ok) }()
	ok, err = f()
	return err
}

// CapacityStatus is the feedback provided by the user to Retry about the underlying resource being managed by Policy.
type CapacityStatus int

const (
	// Within means that the underlying resource is within capacity.
	Within CapacityStatus = iota
	// OverNoRetry means that the underlying resource is over capacity but no retry is needed.
	// This is useful in situations where a request using the resource succeeded, but there are
	// signs of congestion (for example, in the form of high latency).
	OverNoRetry
	// OverNeedRetry means that the underlying resource is over capacity and a retry is needed.
	// This is useful in situations where requests failed due to the underlying resource hitting capacity limits.
	OverNeedRetry
)

// RetryPolicy combines an admission controller with a retry policy.
type RetryPolicy interface {
	Policy
	retry.Policy
}

// Retry calls f after being admitted by the Policy (implied by the given RetryPolicy).
// If f returns Within, true is passed to the underlying policy upon Release and false otherwise.
// If f returns OverNeedRetry, f will be retried as per the RetryPolicy (and the error returned by f is ignored),
// and if f can no longer be retried, the error returned by retry.Policy will be returned.
func Retry(ctx context.Context, policy RetryPolicy, tokens int, f func() (CapacityStatus, error)) error {
	var err error
	for retries := 0; ; retries++ {
		var c CapacityStatus
		err = Do(ctx, policy, tokens, func() (bool, error) {
			var err error // nolint:govet
			c, err = f()
			return c == Within, err
		})
		// Retry as per retry policy if attempt failed due to over capacity.
		if c != OverNeedRetry {
			break
		}
		if err = retry.Wait(ctx, policy, retries); err != nil {
			break
		}
		log.Debug.Printf("admit.Retry: %v, retries=%d", err, retries)
	}
	return err
}

const defaultLimitChangeRate = 0.1

// Adjust changes the limit by factor.
func adjust(limit int, factor float32) int {
	return int(float32(limit) * (1 + factor))
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

type controller struct {
	// limit, used are the current limit and current used tokens respectively.
	limit, used int
	// low, high define the range within which the limit can be adjusted.
	low, high         int
	mu                sync.Mutex
	cond              *ctxsync.Cond
	limitVar, usedVar expvar.Int
}

type controllerWithRetry struct {
	*controller
	retry.Policy
}

func newController(start, limit int) *controller {
	c := &controller{limit: start, used: 0, low: start, high: limit}
	c.cond = ctxsync.NewCond(&c.mu)
	return c
}

// Controller returns a Policy which starts with a concurrency limit of 'start'
// and can grow upto a maximum of 'limit' as long as errors aren't observed.
// A controller is not fair: tokens are not granted in FIFO order;
// rather, waiters are picked randomly to be granted new tokens.
func Controller(start, limit int) Policy {
	return newController(start, limit)
}

// ControllerWithRetry returns a RetryPolicy which starts with a concurrency
// limit of 'start' and can grow upto a maximum of 'limit' if no errors are seen.
// A controller is not fair: tokens are not granted in FIFO order;
// rather, waiters are picked randomly to be granted new tokens.
func ControllerWithRetry(start, limit int, retryPolicy retry.Policy) RetryPolicy {
	return controllerWithRetry{controller: newController(start, limit), Policy: retryPolicy}
}

// EnableVarExport enables the export of relevant vars useful for debugging/monitoring.
func EnableVarExport(policy Policy, name string) {
	switch c := policy.(type) {
	case *controller:
		admitLimit.Set(name, &c.limitVar)
		admitUsed.Set(name, &c.usedVar)
	case *aimd:
		admitLimit.Set(name, &c.limitVar)
		admitUsed.Set(name, &c.usedVar)
	}
}

// Acquire acquires a number of tokens from the admission controller.
// Returns on success, or if the context was canceled.
func (c *controller) Acquire(ctx context.Context, need int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		// TODO(swami): should allow an increase only when the last release was ok
		lim := min(adjust(c.limit, defaultLimitChangeRate), c.high)
		have := lim - c.used
		if need <= have || (need > lim && c.used == 0) {
			c.used += need
			c.usedVar.Set(int64(c.used))
			return nil
		}
		if err := c.cond.Wait(ctx); err != nil {
			return err
		}
	}
}

// Release releases a number of tokens to the admission controller,
// reporting whether the request was within the capacity limits.
func (c *controller) Release(tokens int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ok {
		if c.used > c.limit {
			c.limit = min(c.used, c.high)
		}
	} else {
		c.limit = max(c.low, adjust(c.limit, -defaultLimitChangeRate))
	}
	c.used -= tokens

	c.limitVar.Set(int64(c.limit))
	c.usedVar.Set(int64(c.used))
	c.cond.Broadcast()
}

type aimd struct {
	// limit, used are the current limit and current used tokens respectively.
	limit, used int
	// min is the minimum limit.
	min int
	// decfactor is the factor by which tokens are reduced upon congestion.
	decfactor float32

	mu                sync.Mutex
	cond              *ctxsync.Cond
	limitVar, usedVar expvar.Int
}

type aimdWithRetry struct {
	*aimd
	retry.Policy
}

func newAimd(min int, decfactor float32) *aimd {
	c := &aimd{min: min, limit: min, decfactor: decfactor}
	c.cond = ctxsync.NewCond(&c.mu)
	return c
}

// AIMD returns a Policy which uses the Additive increase/multiplicative decrease
// algorithm for computing the amount of the concurrency to allow.
// AIMD is not fair: tokens are not granted in FIFO order;
// rather, waiters are picked randomly to be granted new tokens.
func AIMD(min int, decfactor float32) Policy {
	return newAimd(min, decfactor)
}

// AIMDWithRetry returns a RetryPolicy which uses the Additive increase/multiplicative decrease
// algorithm for computing the amount of the concurrency to allow.
// AIMDWithRetry is not fair: tokens are not granted in FIFO order;
// rather, waiters are picked randomly to be granted new tokens.
func AIMDWithRetry(min int, decfactor float32, retryPolicy retry.Policy) RetryPolicy {
	return aimdWithRetry{aimd: newAimd(min, decfactor), Policy: retryPolicy}
}

// Acquire acquires a number of tokens from the admission controller.
// Returns on success, or if the context was canceled.
func (c *aimd) Acquire(ctx context.Context, need int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		have := c.limit - c.used
		if need <= have || (need > c.limit && c.used == 0) {
			c.used += need
			c.usedVar.Set(int64(c.used))
			return nil
		}
		if err := c.cond.Wait(ctx); err != nil {
			return err
		}
	}
}

// Release releases a number of tokens to the admission controller,
// reporting whether the request was within the capacity limits.
func (c *aimd) Release(tokens int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case !ok:
		c.limit = max(c.min, adjust(c.limit, -c.decfactor))
	case ok && c.used == c.limit:
		c.limit += 1
	}
	c.used -= tokens

	c.limitVar.Set(int64(c.limit))
	c.usedVar.Set(int64(c.used))
	c.cond.Broadcast()
}
