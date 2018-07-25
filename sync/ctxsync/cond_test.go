// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ctxsync

import (
	"context"
	"sync"
	"testing"
)

func TestContextCond(t *testing.T) {
	var (
		mu          sync.Mutex
		cond        = NewCond(&mu)
		start, done sync.WaitGroup
	)
	const N = 100
	start.Add(N)
	done.Add(N)
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		go func(idx int) {
			mu.Lock()
			start.Done()
			if err := cond.Wait(context.Background()); err != nil {
				errs[idx] = err
			}
			mu.Unlock()
			done.Done()
		}(i)
	}

	start.Wait()
	mu.Lock()
	cond.Broadcast()
	mu.Unlock()
	done.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestContextCondErr(t *testing.T) {
	var (
		mu   sync.Mutex
		cond = NewCond(&mu)
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mu.Lock()
	if got, want := cond.Wait(ctx), context.Canceled; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
