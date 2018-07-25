// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package ttlcache_test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/ttlcache"
)

func TestCache(t *testing.T) {
	for _, test := range []struct {
		setKey   interface{}
		setValue interface{}

		getKey    interface{}
		wantValue interface{}
		wantFound bool
	}{
		{10, "10", 10, "10", true},
		{10, "10", 11, nil, false},
		{10, "10", nil, nil, false},
	} {
		c := ttlcache.New(time.Minute)
		c.Set(test.setKey, test.setValue)
		if gotValue, gotFound := c.Get(test.getKey); (gotFound != test.wantFound) || (gotValue != test.wantValue) {
			t.Errorf("unexpected result for %+v: want (%v, %v), got (%v, %v)", test, test.wantFound, test.wantValue, gotFound, gotValue)
		}
	}
}

func TestCacheTTL(t *testing.T) {
	d := 10 * time.Millisecond
	c := ttlcache.New(d)
	key, value := 10, "10"
	c.Set(key, value)
	if _, gotFound := c.Get(key); !gotFound {
		t.Errorf("missing key in cache: %v", key)
	}
	time.Sleep(d)
	if _, gotFound := c.Get(key); gotFound {
		t.Errorf("unpexpected key in cache: %v", key)
	}
}

// TestCacheConcurrent can fail implicitly by deadlocking.
func TestCacheConcurrent(t *testing.T) {
	c := ttlcache.New(time.Minute)
	wg := sync.WaitGroup{}
	deadline := time.Now().Add(100 * time.Millisecond)
	key := 10
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			loops := 0
			for {
				if time.Now().After(deadline) {
					break
				}
				if rand.Int()%2 == 0 {
					c.Get(key)
				} else {
					c.Set(key, i)
				}
				loops += 1
			}
			wg.Done()
			t.Logf("loops %d: %d", i, loops)
		}(i)
	}
	wg.Wait()
}
