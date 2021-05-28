// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package limiter

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/traverse"
	"golang.org/x/time/rate"
)

type testBatchApi struct {
	mu          sync.Mutex
	usePtr      bool
	maxPerBatch int
	last        time.Time
	perBatchIds [][]string
	durs        []time.Duration
	idSeenCount map[string]int
}

func (a *testBatchApi) MaxPerBatch() int { return a.maxPerBatch }
func (a *testBatchApi) Do(results map[ID]*Result) {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	if a.last.IsZero() {
		a.last = now
	}
	ids := make([]string, 0, len(results))
	for k, r := range results {
		var id string
		if a.usePtr {
			id = *k.(*string)
		} else {
			id = k.(string)
		}
		ids = append(ids, id)
		idSeenCount := a.idSeenCount[id]
		i, err := strconv.Atoi(id)
		if err != nil {
			i = -1
		}
		switch {
		case shouldErr(i):
		case i%2 == 0:
			r.Set(nil, fmt.Errorf("failed_%s_count_%d", id, idSeenCount))
		default:
			r.Set(fmt.Sprintf("value-%s", id), nil)
		}
		a.idSeenCount[id] = idSeenCount + 1
	}
	a.perBatchIds = append(a.perBatchIds, ids)
	a.durs = append(a.durs, now.Sub(a.last))
	a.last = now
	return
}

func TestSimple(t *testing.T) {
	a := &testBatchApi{idSeenCount: make(map[string]int)}
	l := NewBatchLimiter(a, rate.NewLimiter(rate.Every(time.Millisecond), 1))
	id := "test"
	_, _ = l.Do(context.Background(), id)
	if got, want := a.idSeenCount[id], 1; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
	_, _ = l.Do(context.Background(), id)
	if got, want := a.idSeenCount[id], 2; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

func TestCtxCanceled(t *testing.T) {
	a := &testBatchApi{idSeenCount: make(map[string]int)}
	l := NewBatchLimiter(a, rate.NewLimiter(rate.Every(time.Second), 1))
	id1, id2 := "test1", "test2"
	_, _ = l.Do(context.Background(), id1)
	if got, want := a.idSeenCount[id1], 1; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = l.Do(ctx, id1)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = l.Do(context.Background(), id2)
	}()
	wg.Wait()
	if got, want := a.idSeenCount[id1], 1; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
	if got, want := a.idSeenCount[id2], 1; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

func TestSometimesDedup(t *testing.T) {
	a := &testBatchApi{idSeenCount: make(map[string]int)}
	l := NewBatchLimiter(a, rate.NewLimiter(rate.Every(10*time.Millisecond), 1))
	id := "test"
	a.mu.Lock() // Locks the batch API.
	var started, done sync.WaitGroup
	started.Add(5)
	done.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			started.Done()
			_, _ = l.Do(context.Background(), id)
			done.Done()
		}()
	}
	started.Wait() // Wait for all the goroutines on the same ID to start
	a.mu.Unlock()  // Unlock the batch API.
	done.Wait()    // Wait for all the goroutines on the same ID to complete
	if got, want := a.idSeenCount[id], 2; got > want {
		t.Errorf("got %d, want <=%d", got, want)
	}
}

func TestNoDedup(t *testing.T) {
	a := &testBatchApi{usePtr: true, idSeenCount: make(map[string]int)}
	l := NewBatchLimiter(a, rate.NewLimiter(rate.Every(10*time.Millisecond), 1))
	id := "test"
	a.mu.Lock() // Locks the batch API.
	var started, done sync.WaitGroup
	started.Add(5)
	done.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			started.Done()
			id := id
			_, _ = l.Do(context.Background(), &id)
			done.Done()
		}()
	}
	started.Wait() // Wait for all the goroutines on the same ID to start
	a.mu.Unlock()  // Unlock the batch API.
	done.Wait()    // Wait for all the goroutines on the same ID to complete
	if got, want := a.idSeenCount[id], 5; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

func TestDo(t *testing.T) {
	testApi(t, &testBatchApi{idSeenCount: make(map[string]int)}, time.Second)
}

func TestDoWithMax5(t *testing.T) {
	testApi(t, &testBatchApi{maxPerBatch: 5, idSeenCount: make(map[string]int)}, 3*time.Second)
}

func TestDoWithMax8(t *testing.T) {
	testApi(t, &testBatchApi{maxPerBatch: 8, idSeenCount: make(map[string]int)}, 2*time.Second)
}

type result struct {
	v   string
	err error
}

func shouldErr(i int) bool {
	return i%5 == 0 && i%2 != 0
}

func testApi(t *testing.T, a *testBatchApi, timeout time.Duration) {
	const numIds = 100
	var interval = 100 * time.Millisecond
	l := NewBatchLimiter(a, rate.NewLimiter(rate.Every(interval), 1))
	var mu sync.Mutex
	results := make(map[string]result)
	_ = traverse.Each(numIds, func(i int) error {
		time.Sleep(time.Duration(i*10) * time.Millisecond)
		id := fmt.Sprintf("%d", i)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		v, err := l.Do(ctx, id)
		mu.Lock()
		r := result{err: err}
		if r.err == nil {
			r.v = v.(string)
		}
		results[id] = r
		mu.Unlock()
		return nil
	})
	for i := 0; i < numIds; i++ {
		id := fmt.Sprintf("%d", i)
		if got, want := a.idSeenCount[id], 1; got != want {
			t.Errorf("[%v] got %d, want %d", id, got, want)
		}
		if shouldErr(i) {
			if got, want := results[id].err, ErrNoResult; got != want {
				t.Errorf("[%d] got %v, want %v", i, got, want)
			}
		}
	}
	for _, dur := range a.durs[1:] {
		if got, want, diff := dur, interval, (dur - interval).Round(5*time.Millisecond); diff < 0 {
			t.Errorf("got %v, want %v, diff %v", got, want, diff)
		}
	}
	for i, batchIds := range a.perBatchIds {
		if want := a.maxPerBatch; want > 0 {
			if got := len(batchIds); got > want {
				t.Errorf("got %v, want <=%v", got, want)
			}
		}
		t.Logf("batch %d (after %s): %v", i, a.durs[i].Round(time.Millisecond), batchIds)
	}
}
