// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package once contains utilities for managing actions that
// must be performed exactly once.
package once

import (
	"sync"
	"sync/atomic"
)

// Task manages a computation that must be run at most once.
// It's similar to sync.Once, except it also handles and returns errors.
type Task struct {
	mu   sync.Mutex
	done uint32
	err  error
}

// Do run the function do at most once. Successive invocations of Do
// guarantee exactly one invocation of the function do. Do returns
// the error of do's invocation.
func (o *Task) Do(do func() error) error {
	if atomic.LoadUint32(&o.done) == 1 {
		return o.err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if atomic.LoadUint32(&o.done) == 0 {
		o.err = do()
		atomic.StoreUint32(&o.done, 1)
	}
	return o.err
}

// Done returns whether the task is done.
func (o *Task) Done() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return 1 == atomic.LoadUint32(&o.done)
}

// Reset resets the task effectively making it possible for `Do` to invoke the underlying do func again.
// Reset will only reset the task if it was already completed.
func (o *Task) Reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	atomic.CompareAndSwapUint32(&o.done, 1, 0)
}

// Map coordinates actions that must happen exactly once, keyed
// by user-defined keys.
type Map sync.Map

// Perform the provided action named by a key. Do invokes the action
// exactly once for each key, and returns any errors produced by the
// provided action.
func (m *Map) Do(key interface{}, do func() error) error {
	taskv, _ := (*sync.Map)(m).LoadOrStore(key, new(Task))
	task := taskv.(*Task)
	return task.Do(do)
}

// Forget forgets past computations associated with the provided key.
func (m *Map) Forget(key interface{}) {
	(*sync.Map)(m).Delete(key)
}
