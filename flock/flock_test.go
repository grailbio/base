package flock_test

import (
	"io/ioutil"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grailbio/base/flock"
)

func TestLock(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}

	lockPath := tempDir + "/lock"
	lock := flock.New(lockPath)

	// Test uncontended locks
	for i := 0; i < 3; i++ {
		lock.Lock()
		lock.Unlock()
	}

	lock.Lock()

	locked := int64(0)
	doneCh := make(chan struct{})
	go func() {
		lock.Lock()
		atomic.StoreInt64(&locked, 1)
		lock.Unlock()
		atomic.StoreInt64(&locked, 2)
		doneCh <- struct{}{}
	}()

	time.Sleep(500 * time.Millisecond)
	if atomic.LoadInt64(&locked) != 0 {
		t.Errorf("locked=%d", locked)
	}

	lock.Unlock()
	<-doneCh
	if atomic.LoadInt64(&locked) != 2 {
		t.Errorf("locked=%d", locked)
	}
}
