// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package limiter implements a concurrency limiter with support for
// contexts.
package limiter

import "context"

// A Limiter enforces concurrency limits among a set of goroutines.
// It maintains a bucket of tokens; a number of tokens (e.g.,
// representing the cost of an operation) must be acquired by a
// goroutine before proceeding. A limiter is not fair: tokens are not
// granted in FIFO order; rather, waiters are picked randomly to be
// granted new tokens.
//
// A nil limiter issues an infinite number of tokens.
type Limiter struct {
	c      chan int
	waiter chan struct{}
}

// New creates a new limiter with 0 tokens.
func New() *Limiter {
	l := &Limiter{make(chan int, 1), make(chan struct{}, 1)}
	l.waiter <- struct{}{}
	return l
}

// Acquire blocks until the goroutine is granted the desired number
// of tokens, or until the context is done.
func (l *Limiter) Acquire(ctx context.Context, need int) error {
	if l == nil {
		return ctx.Err()
	}
	select {
	case <-l.waiter:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() {
		l.waiter <- struct{}{}
	}()

	var have int
	for {
		select {
		case n := <-l.c:
			have += n
			if m := have - need; m >= 0 {
				l.Release(m)
				return nil
			}
		case <-ctx.Done():
			l.Release(have)
			return ctx.Err()
		}
	}
}

// Release adds a number of tokens back into the limiter.
func (l *Limiter) Release(n int) {
	if l == nil {
		return
	}
	if n == 0 {
		return
	}
	for {
		select {
		case l.c <- n:
			return
		case have := <-l.c:
			n += have
		}
	}
}

type LimiterIfc interface {
	Release(n int)
	Acquire(ctx context.Context, need int) error
}
