// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package limiter

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grailbio/base/traverse"
)

func TestLimiter(t *testing.T) {
	l := New()
	l.Release(10)

	if err := l.Acquire(context.Background(), 5); err != nil {
		t.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	if want, got := context.DeadlineExceeded, l.Acquire(ctx, 10); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	l.Release(5)
	if err := l.Acquire(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
}

func TestLimiterConcurrently(t *testing.T) {
	const (
		N = 1000
		T = 100
	)
	var pending int32
	l := New()
	l.Release(T)
	var begin sync.WaitGroup
	begin.Add(N)
	err := traverse.Each(N, func(i int) error {
		begin.Done()
		begin.Wait()
		n := rand.Intn(T) + 1
		if err := l.Acquire(context.Background(), n); err != nil {
			return err
		}
		if m := atomic.AddInt32(&pending, int32(n)); m > T {
			return fmt.Errorf("too many tokens: %d > %d", m, T)
		}
		atomic.AddInt32(&pending, -int32(n))
		l.Release(n)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
