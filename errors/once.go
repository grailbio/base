// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package errors

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// Once captures at most one error. Errors are safely set across
// multiple goroutines.
//
// A zero Once is ready to use.
//
// Example:
// 	var e errors.Once
// 	e.Set(errors.New("test error 0"))
type Once struct {
	// Ignored is a list of errors that will be dropped in Set(). Ignored
	// typically includes io.EOF.
	Ignored []error
	mu      sync.Mutex
	err     unsafe.Pointer // stores *error
}

// Err returns the first non-nil error passed to Set.  Calling Err is
// cheap (~1ns).
func (e *Once) Err() error {
	p := atomic.LoadPointer(&e.err) // Acquire load
	if p == nil {
		return nil
	}
	return *(*error)(p)
}

// Set sets this instance's error to err. Only the first error
// is set; subsequent calls are ignored.
func (e *Once) Set(err error) {
	if err != nil {
		for _, ignored := range e.Ignored {
			if err == ignored {
				return
			}
		}
		e.mu.Lock()
		if e.err == nil && err != nil {
			atomic.StorePointer(&e.err, unsafe.Pointer(&err)) // Release store
		}
		e.mu.Unlock()
	}
}
