// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package admit

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grailbio/base/retry"
	"github.com/grailbio/base/traverse"
)

func checkState(t *testing.T, c *controller, max, used int) {
	t.Helper()
	if c.used != used {
		t.Errorf("c.used: got %d, want %d", c.used, used)
	}
	if c.max != max {
		t.Errorf("c.max: got %d, want %d", c.max, max)
	}
}

func TestController(t *testing.T) {
	c := newController(10, 15, nil)
	// use up 5.
	if err := c.Acquire(context.Background(), 5); err != nil {
		t.Fatal(err)
	}
	checkState(t, c, 10, 5)
	// can go upto 6.
	if err := c.Acquire(context.Background(), 6); err != nil {
		t.Fatal(err)
	}
	// release and report capacity error.
	c.Release(5, false)
	checkState(t, c, 9, 6)
	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
	// 6 still in use and max should now be 9, so can't acquire 4.
	if want, got := context.DeadlineExceeded, c.Acquire(ctx, 4); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	c.Release(6, true)
	checkState(t, c, 9, 0)
	// max is still 9, but since none are used, should accommodate larger request.
	if err := c.Acquire(context.Background(), 18); err != nil {
		t.Fatal(err)
	}
	checkState(t, c, 9, 18)
	c.Release(17, true)
	checkState(t, c, 15, 1)
	ctx, _ = context.WithTimeout(context.Background(), time.Millisecond)
	// 1 still in use and max is 15, so shouldn't accommodate larger request.
	if want, got := context.DeadlineExceeded, c.Acquire(ctx, 18); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	checkState(t, c, 15, 1)
	c.Release(1, true)
	checkState(t, c, 15, 0)
}

func TestControllerConcurrently(t *testing.T) {
	const (
		N = 100
		T = 100
	)
	var pending int32
	c := Controller(100, 1000)
	var begin sync.WaitGroup
	begin.Add(N)
	err := traverse.Each(N, func(i int) error {
		begin.Done()
		n := rand.Intn(T/10) + 1
		if err := c.Acquire(context.Background(), n); err != nil {
			return err
		}
		if m := atomic.AddInt32(&pending, int32(n)); m > T {
			return fmt.Errorf("too many tokens: %d > %d", m, T)
		}
		atomic.AddInt32(&pending, -int32(n))
		c.Release(n, (i > 10 && i < 20) || (i > 70 && i < 80))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDo(t *testing.T) {
	someErr := errors.New("some other error")
	c := newController(100, 10000, nil)
	// Must satisfy even 150 tokens since none are used.
	if err := Do(context.Background(), c, 150, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	checkState(t, c, 150, 0)
	// controller has 150 tokens, use 10 and report capacity error
	if want, got := ErrOverCapacity, Do(context.Background(), c, 10, func() error { return ErrOverCapacity }); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	checkState(t, c, 135, 0)
	// controller has 135 tokens, use up 35...
	c.Acquire(context.Background(), 35)
	checkState(t, c, 135, 35)
	// can go upto 1.1*135 = 148, so should timeout for 114.
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	if want, got := context.DeadlineExceeded, Do(ctx, c, 114, func() error { return someErr }); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	checkState(t, c, 135, 35)
	// can go upto 1.1*135 = 148, so should timeout for 113.
	if want, got := someErr, Do(context.Background(), c, 113, func() error { return someErr }); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	checkState(t, c, 148, 35)
	// can go upto 1.1*148 = 162, so should go upto 127.
	if err := Do(context.Background(), c, 127, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestRetry(t *testing.T) {
	const (
		N = 1000
	)
	c := ControllerWithRetry(200, 1000, retry.MaxTries(retry.Backoff(100*time.Millisecond, time.Minute, 1.5), 5))
	var begin sync.WaitGroup
	begin.Add(N)
	err := traverse.Each(N, func(i int) error {
		begin.Done()
		begin.Wait()
		randFunc := func() error {
			if i%2 == 0 { // Every other request can potentially fail ...
				time.Sleep(time.Millisecond * time.Duration(20+rand.Intn(50)))
				if rand.Intn(100) < 5 { // 5% of the time.
					return ErrOverCapacity
				}
			}
			time.Sleep(time.Millisecond * time.Duration(5+rand.Intn(20)))
			return nil
		}
		n := rand.Intn(20) + 1
		return Retry(context.Background(), c, n, randFunc)
	})
	if err != nil {
		t.Fatal(err)
	}
}
