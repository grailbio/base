// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ctxsync

import (
	"context"
	"sync"

	"github.com/grailbio/base/errors"
)

// Mutex is a context-aware mutex.  It must not be copied.
// The zero value is ready to use.
type Mutex struct {
	initOnce sync.Once
	lockCh   chan struct{}
}

// Lock attempts to exclusively lock m.  If the m is already locked, it will
// wait until it is unlocked.  If ctx is canceled before the lock can be taken,
// Lock will not take the lock, and a non-nil error is returned.
func (m *Mutex) Lock(ctx context.Context) error {
	m.init()
	select {
	case m.lockCh <- struct{}{}:
		return nil
	case <-ctx.Done():
		return errors.E(ctx.Err(), "waiting for lock")
	}
}

// Unlock unlocks m.  It must be called exactly once iff Lock returns nil.
// Unlock panics if it is called while m is not locked.
func (m *Mutex) Unlock() {
	m.init()
	select {
	case <-m.lockCh:
	default:
		panic("Unlock called on mutex that is not locked")
	}
}

func (m *Mutex) init() {
	m.initOnce.Do(func() {
		m.lockCh = make(chan struct{}, 1)
	})
}
