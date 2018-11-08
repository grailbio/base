// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package admit contains utilities for admission control.
package admit

import (
	"context"
	"errors"
	"expvar"
	"sync"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/base/sync/ctxsync"
)

var (
	controllerMax  = expvar.NewMap("controller.max")
	controllerUsed = expvar.NewMap("controller.used")
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

// RetryPolicy combines an admission controller with a retry policy.
type RetryPolicy interface {
	Policy
	retry.Policy
}

// ErrOverCapacity should be thrown by the "do" func passed to admit.Do or admit.Retry
// for it to be considered an over capacity error.
var ErrOverCapacity = errors.New("over capacity")

// Do calls the provided function after being admitted by the admission controller.
// If the function returns ErrOverCapacity, it is then reported as a capacity request
// to the underlying policy.
func Do(ctx context.Context, policy Policy, tokens int, do func() error) error {
	if err := policy.Acquire(ctx, tokens); err != nil {
		return err
	}
	var err error
	defer func(err *error) {
		policy.Release(tokens, *err != ErrOverCapacity)
	}(&err)
	err = do()
	return err
}

// Retry calls the provided function with the combined retry and admission policies.
func Retry(ctx context.Context, policy RetryPolicy, tokens int, do func() error) error {
	var err error
	for retries := 0; ; retries++ {
		err = Do(ctx, policy, tokens, do)
		// Retry as per retry policy if attempt failed due to over capacity.
		if err != ErrOverCapacity {
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

// Adjust either increases or decreases the given limit by defaultChangeRate.
func adjust(limit int, increase bool) int {
	var change float32
	if increase {
		change = 1.0 + defaultLimitChangeRate
	} else {
		change = 1.0 - defaultLimitChangeRate
	}
	return int(float32(limit) * change)
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

type controller struct {
	retry.Policy
	max, used, limit int
	mu               sync.Mutex
	cond             *ctxsync.Cond
	varKey           string
	maxVar, usedVar  expvar.Var
}

func newController(start, limit int, retryPolicy retry.Policy) *controller {
	c := &controller{Policy: retryPolicy, max: start, used: 0, limit: limit}
	c.cond = ctxsync.NewCond(&c.mu)
	return c
}

// Controller returns a Policy which starts with a concurrency limit of 'start'
// and can grow upto a maximum of 'limit' as long as errors aren't observed.
// A controller is not fair: tokens are not granted in FIFO order;
// rather, waiters are picked randomly to be granted new tokens.
func Controller(start, limit int) Policy {
	return ControllerWithRetry(start, limit, nil)
}

// ControllerWithRetry returns a RetryPolicy which starts with a concurrency
// limit of 'start' and can grow upto a maximum of 'limit' if no errors are seen.
// A controller is not fair: tokens are not granted in FIFO order;
// rather, waiters are picked randomly to be granted new tokens.
func ControllerWithRetry(start, limit int, retryPolicy retry.Policy) RetryPolicy {
	return newController(start, limit, retryPolicy)
}

// WithVarExport initiates export of relevant vars.
func WithVarExport(policy RetryPolicy, name string) RetryPolicy {
	switch c := policy.(type) {
	case *controller:
		c.varKey = name
		return c
	}
	return policy
}

// Acquire acquires a number of tokens from the admission controller.
// Returns on success, or if the context was canceled.
func (c *controller) Acquire(ctx context.Context, need int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		// TODO(swami): should allow an increase only when the last release was ok
		lim := min(adjust(c.max, true), c.limit)
		have := lim - c.used
		if need <= have || (need > lim && c.used == 0) {
			c.used += need
			if c.varKey != "" {
				setVar(controllerUsed, c.varKey, c.used)
			}
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
		if c.used > c.max {
			c.max = min(c.used, c.limit)
		}
	} else {
		c.max = adjust(c.max, false)
	}
	c.used -= tokens

	if c.varKey != "" {
		setVar(controllerMax, c.varKey, c.max)
		setVar(controllerUsed, c.varKey, c.used)
	}
	c.cond.Broadcast()
}

func setVar(m *expvar.Map, key string, val int) {
	i := &expvar.Int{}
	i.Set(int64(val))
	m.Set(key, i)
}
