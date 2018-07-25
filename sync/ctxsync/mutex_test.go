// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ctxsync_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/sync/ctxsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// TestExclusion verifies that a mutex provides basic mutually exclusive
// access: only one goroutine can have it locked at a time.
func TestExclusion(t *testing.T) {
	var (
		mu ctxsync.Mutex
		wg sync.WaitGroup
		x  int
	)
	require.NoError(t, mu.Lock(context.Background()))
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := mu.Lock(context.Background()); err != nil {
			return
		}
		x = 100
		mu.Unlock()
	}()
	for i := 1; i <= 10; i++ {
		// Verify that nothing penetrates our lock and changes x unexpectedly.
		assert.Equal(t, i-1, x)
		x = i
		time.Sleep(1 * time.Millisecond)
	}
	mu.Unlock()
	wg.Wait()
	assert.Equal(t, 100, x)
}

// TestOtherGoroutineUnlock verifies that locked mutexes can be unlocked by a
// different goroutine, and that the lock still provides mutual exclusion
// across them.
func TestOtherGoroutineUnlock(t *testing.T) {
	const N = 100
	var (
		mu       ctxsync.Mutex
		g        errgroup.Group
		chLocked = make(chan struct{})
		x        int
	)
	// Run N goroutines each trying to lock the mutex.  Run another N
	// goroutines, one of which is selected to unlock the mutex after each time
	// it is successfully locked.
	for i := 0; i < N; i++ {
		g.Go(func() error {
			if err := mu.Lock(context.Background()); err != nil {
				return err
			}
			x++
			chLocked <- struct{}{}
			return nil
		})
		g.Go(func() error {
			<-chLocked
			x++
			mu.Unlock()
			return nil
		})
	}
	assert.NoError(t, g.Wait())
	// We run N*2 goroutines, each incrementing x by 1 while the lock is held.
	assert.Equal(t, N*2, x)
}

// TestCancel verifies that canceling the Lock context causes the attempt to
// lock the mutex to fail and return an error of kind errors.Canceled.
func TestCancel(t *testing.T) {
	var (
		mu        ctxsync.Mutex
		wg        sync.WaitGroup
		errWaiter error
	)
	require.NoError(t, mu.Lock(context.Background()))
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		if errWaiter = mu.Lock(ctx); errWaiter != nil {
			return
		}
		mu.Unlock()
	}()
	cancel()
	wg.Wait()
	mu.Unlock()
	// Verify that we can still lock and unlock after the canceled attempt.
	if assert.NoError(t, mu.Lock(context.Background())) {
		mu.Unlock()
	}
	// Verify that Lock returned the expected non-nil error from the canceled
	// attempt.
	assert.True(t, errors.Is(errors.Canceled, errWaiter), "expected errors.Canceled")
}

// TestUnlockUnlocked verifies that unlocking a mutex that is not locked
// panics.
func TestUnlockUnlocked(t *testing.T) {
	var mu ctxsync.Mutex
	assert.Panics(t, func() { mu.Unlock() })
}
