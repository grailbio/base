package flock_test

import (
	"context"
	"io/ioutil"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grailbio/base/flock"
	"github.com/grailbio/testutil/assert"
)

func TestLock(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}

	lockPath := tempDir + "/lock"
	lock := flock.New(lockPath)

	// Test uncontended locks
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		assert.NoError(t, lock.Lock(ctx))
		assert.NoError(t, lock.Unlock())
	}

	assert.NoError(t, lock.Lock(ctx))
	locked := int64(0)
	doneCh := make(chan struct{})
	go func() {
		assert.NoError(t, lock.Lock(ctx))
		atomic.StoreInt64(&locked, 1)
		assert.NoError(t, lock.Unlock())
		atomic.StoreInt64(&locked, 2)
		doneCh <- struct{}{}
	}()

	time.Sleep(500 * time.Millisecond)
	if atomic.LoadInt64(&locked) != 0 {
		t.Errorf("locked=%d", locked)
	}

	assert.NoError(t, lock.Unlock())
	<-doneCh
	if atomic.LoadInt64(&locked) != 2 {
		t.Errorf("locked=%d", locked)
	}
}

func TestLockContext(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	lockPath := tempDir + "/lock"

	lock := flock.New(lockPath)
	ctx := context.Background()
	ctx2, cancel2 := context.WithCancel(ctx)
	assert.NoError(t, lock.Lock(ctx2))
	assert.NoError(t, lock.Unlock())

	assert.NoError(t, lock.Lock(ctx))
	go func() {
		time.Sleep(500 * time.Millisecond)
		cancel2()
	}()
	assert.Regexp(t, lock.Lock(ctx2), "context canceled")

	assert.NoError(t, lock.Unlock())
	// Make sure the lock is in a sane state by cycling lock-unlock again.
	assert.NoError(t, lock.Lock(ctx))
	assert.NoError(t, lock.Unlock())
}
