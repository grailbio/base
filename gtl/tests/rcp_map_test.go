package tests

//go:generate ../generate.py --prefix=rcuTest --PREFIX=RCUTest -DKEY=string -DVALUE=uint64 -DHASH=testhash --package=tests --output=rcu_map.go ../rcu_map.go.tpl

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grailbio/testutil/assert"
)

type modelMap map[string]uint64

func testRCUMap(t *testing.T, r *rand.Rand) {
	m := NewRCUTestMap(r.Intn(100))
	model := modelMap{}

	for i := 0; i < 1000; i++ {
		op := r.Intn(3)
		if op == 2 {
			key := fmt.Sprintf("key%d", r.Intn(1000))
			val := r.Uint64()
			m.Store(key, val)
			model[key] = val
			continue
		}
		key := fmt.Sprintf("key%d", r.Intn(1000))
		if val, ok := model[key]; ok {
			val2, ok2 := m.Load(key)
			assert.EQ(t, ok2, ok, key)
			assert.EQ(t, val2, val, key)
		}
	}
}

func TestRCUMap(t *testing.T) {
	for seed := 0; seed < 1000; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			testRCUMap(t, rand.New(rand.NewSource(int64(seed))))
		})
	}
}

func TestConcurrentRCUMap(t *testing.T) {
	var (
		seq  uint64
		done uint64
		wg   sync.WaitGroup
		m    = NewRCUTestMap(10)
	)

	// Producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		start := time.Now()
		for time.Since(start) < 5*time.Second {
			val := seq
			key := fmt.Sprintf("key%d", val)
			m.Store(key, val)
			atomic.StoreUint64(&seq, val+1)
			time.Sleep(time.Millisecond)
		}
		atomic.StoreUint64(&done, 1)
	}()

	// Consumer
	for seed := 0; seed < 10; seed++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(seed)))
			ops := 0
			for atomic.LoadUint64(&done) == 0 {
				floor := atomic.LoadUint64(&seq) // A key < floor is guaranteed to be in the map
				if floor == 0 {
					time.Sleep(time.Millisecond)
					continue
				}
				want := uint64(r.Intn(int(floor)))
				key := fmt.Sprintf("key%d", want)
				got, ok := m.Load(key)
				ceil := atomic.LoadUint64(&seq) // A key > ceil is guaranteed not to be in the map
				if ok {
					assert.EQ(t, got, want)
					assert.LE(t, want, ceil)
				} else {
					assert.GE(t, want, floor)
				}
				ops++
			}
			t.Logf("Ops: %d", ops)
			assert.GT(t, ops, 0)
		}(seed)
	}

	wg.Wait()
}
