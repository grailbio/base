// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package admit

import (
	"context"
	"expvar"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grailbio/base/retry"
	"github.com/grailbio/base/traverse"
)

func checkState(t *testing.T, p Policy, limit, used int) {
	t.Helper()
	var gotl, gotu int
	switch c := p.(type) {
	case *controller:
		gotl = c.limit
		gotu = c.used
	case *aimd:
		gotl = c.limit
		gotu = c.used
	}
	if gotu != used {
		t.Errorf("c.used: got %d, want %d", gotu, used)
	}
	if gotl != limit {
		t.Errorf("c.limit: got %d, want %d", gotl, limit)
	}
}

func checkVars(t *testing.T, key, max, used string) {
	t.Helper()
	if want, got := max, admitLimit.Get(key).String(); got != want {
		t.Errorf("admitLimit got %s, want %s", got, want)
	}
	if want, got := used, admitUsed.Get(key).String(); got != want {
		t.Errorf("admitUsed got %s, want %s", got, want)
	}
}

func getKeys(m *expvar.Map) map[string]bool {
	keys := make(map[string]bool)
	m.Do(func(kv expvar.KeyValue) {
		keys[kv.Key] = true
	})
	return keys
}

func TestController(t *testing.T) {
	c := newController(10, 15)
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
	checkState(t, c, 10, 6)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	// 6 still in use and limit should now be 10, so can't acquire 6.
	if want, got := context.DeadlineExceeded, c.Acquire(ctx, 6); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	cancel()
	if want, got := 0, getKeys(admitLimit); len(got) != want {
		t.Fatalf("admitLimit got %v, want len %d", got, want)
	}
	if want, got := 0, getKeys(admitUsed); len(got) != want {
		t.Fatalf("admitUsed got %v, want len %d", got, want)
	}
	EnableVarExport(c, "test")
	c.Release(6, true)
	checkState(t, c, 10, 0)
	checkVars(t, "test", "10", "0")
	// max is still 9, but since none are used, should accommodate larger request.
	if err := c.Acquire(context.Background(), 18); err != nil {
		t.Fatal(err)
	}
	checkState(t, c, 10, 18)
	checkVars(t, "test", "10", "18")
	c.Release(17, true)
	checkState(t, c, 15, 1)
	checkVars(t, "test", "15", "1")
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	// 1 still in use and max is 15, so shouldn't accommodate larger request.
	if want, got := context.DeadlineExceeded, c.Acquire(ctx, 18); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	cancel()
	checkState(t, c, 15, 1)
	checkVars(t, "test", "15", "1")
	c.Release(1, true)
	checkState(t, c, 15, 0)
	checkVars(t, "test", "15", "0")
}

func TestControllerConcurrently(t *testing.T) {
	testPolicy(t, ControllerWithRetry(100, 1000, nil))
}

func TestAIMD(t *testing.T) {
	c := newAimd(10, 0.2)
	// use up 5.
	if err := c.Acquire(context.Background(), 5); err != nil {
		t.Fatal(err)
	}
	checkState(t, c, 10, 5)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	// 5 in use and limit should still be 10, so can't acquire 6.
	if want, got := context.DeadlineExceeded, c.Acquire(ctx, 6); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	cancel()
	// release and report capacity error.
	EnableVarExport(c, "aimd")
	c.Release(5, true)
	checkState(t, c, 10, 0)
	checkVars(t, "aimd", "10", "0")

	for i := 0; i < 10; i++ {
		if err := c.Acquire(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	checkState(t, c, 10, 10)
	checkVars(t, "aimd", "10", "10")
	for i := 1; i <= 5; i++ {
		c.Release(i, true)
		if err := c.Acquire(context.Background(), i+1); err != nil {
			t.Fatal(err)
		}
	}
	checkState(t, c, 15, 15)
	checkVars(t, "aimd", "15", "15")

	c.Release(1, false)
	checkState(t, c, 12, 14)
	checkVars(t, "aimd", "12", "14")

	c.Release(1, false)
	checkState(t, c, 10, 13)
	checkVars(t, "aimd", "10", "13")

	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	// 13 still in use and limit should now be 10, so can't acquire 1.
	if want, got := context.DeadlineExceeded, c.Acquire(ctx, 1); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	cancel()
}

func TestAIMDConcurrently(t *testing.T) {
	testPolicy(t, AIMDWithRetry(100, 0.25, nil))
}

func testPolicy(t *testing.T, p Policy) {
	const (
		N = 100
		T = 100
	)
	var pending int32
	var begin sync.WaitGroup
	begin.Add(N)
	err := traverse.Each(N, func(i int) error {
		begin.Done()
		n := rand.Intn(T/10) + 1
		if err := p.Acquire(context.Background(), n); err != nil {
			return err
		}
		if m := atomic.AddInt32(&pending, int32(n)); m > T {
			return fmt.Errorf("too many tokens: %d > %d", m, T)
		}
		atomic.AddInt32(&pending, -int32(n))
		p.Release(n, (i > 10 && i < 20) || (i > 70 && i < 80))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDo(t *testing.T) {
	c := newController(100, 10000)
	// Must satisfy even 150 tokens since none are used.
	if err := Do(context.Background(), c, 150, func() (bool, error) { return true, nil }); err != nil {
		t.Fatal(err)
	}
	checkState(t, c, 150, 0)
	// controller has 150 tokens, use 10 and report capacity error
	if want, got := error(nil), Do(context.Background(), c, 10, func() (bool, error) { return false, nil }); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	checkState(t, c, 135, 0)
	// controller has 135 tokens, use up 35...
	c.Acquire(context.Background(), 35)
	checkState(t, c, 135, 35)
	// can go upto 1.1*135 = 148, so should timeout for 114.
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	if want, got := context.DeadlineExceeded, Do(ctx, c, 114, func() (bool, error) { return true, nil }); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	checkState(t, c, 135, 35)
	// can go upto 1.1*135 = 148, so should timeout for 113.
	if want, got := error(nil), Do(context.Background(), c, 113, func() (bool, error) { return true, nil }); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	checkState(t, c, 148, 35)
	// can go upto 1.1*148 = 162, so should go upto 127.
	if err := Do(context.Background(), c, 127, func() (bool, error) { return true, nil }); err != nil {
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
		randFunc := func() (CapacityStatus, error) {
			// Out of every three requests, one will (5% of the time) report over capacity with a need to retry,
			// and another (also 5% of the time) will report over capacity with no need to retry.
			switch i % 3 {
			case 0:
				time.Sleep(time.Millisecond * time.Duration(20+rand.Intn(50)))
				if rand.Intn(100) < 5 { // 5% of the time.
					return OverNeedRetry, nil
				}
			case 1:
				time.Sleep(time.Millisecond * time.Duration(20+rand.Intn(50)))
				if rand.Intn(100) < 5 { // 5% of the time.
					return OverNoRetry, nil
				}
			}
			time.Sleep(time.Millisecond * time.Duration(5+rand.Intn(20)))
			return Within, nil
		}
		n := rand.Intn(20) + 1
		return Retry(context.Background(), c, n, randFunc)
	})
	if err != nil {
		t.Fatal(err)
	}
}
