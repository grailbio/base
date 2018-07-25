// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package state

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/grailbio/testutil"
)

// TODO(marius): provide an example test

func mustOpen(t *testing.T, prefix string) *File {
	f, err := Open(prefix)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func mustLock(t *testing.T, f *File) {
	if err := f.Lock(); err != nil {
		t.Fatal(err)
	}
}

func mustUnlock(t *testing.T, f *File) {
	if err := f.Unlock(); err != nil {
		t.Fatal(err)
	}
}

func TestFile(t *testing.T) {
	type mystate struct {
		A, B, C string
	}
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	prefix := filepath.Join(dir, "mystate")
	f1 := mustOpen(t, prefix)
	defer f1.Close()
	f2 := mustOpen(t, prefix)
	defer f2.Close()
	var s1, s2 mystate
	if got, want := f1.Unmarshal(&s1), ErrNoState; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	s1 = mystate{"A", "B", "C"}
	if err := f1.Marshal(&s1); err != nil {
		t.Fatal(err)
	}
	if err := f2.Unmarshal(&s2); err != nil {
		t.Fatal(err)
	}
	if got, want := s2, s1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	// Can't marshal a func.
	if f1.Marshal(func() {}) == nil {
		t.Fatal("expected error")
	}
	// But this shouldn't affect the state.
	if err := f2.Unmarshal(&s2); err != nil {
		t.Fatal(err)
	}
	if got, want := s2, s1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestLock(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	prefix := filepath.Join(dir, "mystate")
	f1 := mustOpen(t, prefix)
	f2 := mustOpen(t, prefix)

	mustLock(t, f1)

	ch := make(chan bool)
	go func() {
		mustLock(t, f2)
		ch <- true
		mustUnlock(t, f2)
	}()

	// Make sure that f2 has not acquired the lock.
	time.Sleep(1 * time.Second)
	select {
	case <-ch:
		t.Fatal("F2 should not have acquired a lock")
	default:
	}
	mustUnlock(t, f1)
	_ = <-ch // make sure that f2 acquires the lock
}
