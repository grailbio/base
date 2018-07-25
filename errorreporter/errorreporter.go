// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package errorreporter is used to accumulate errors from
// multiple threads.
package errorreporter

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// T accumulates errors across multiple threads.  Thread safe.
//
// Example:
//  e := errorreporter.T{}
//  e.Set(errors.New("test error 0"))
type T struct {
	// Ignored is a list of errors that will be dropped in Set(). Ignored
	// typically includes io.EOF.
	Ignored []error
	mu      sync.Mutex
	err     unsafe.Pointer // stores *error
}

// Err returns the first non-nil error passed to Set.  Calling Err is cheap
// (~1ns).
func (e *T) Err() error {
	p := atomic.LoadPointer(&e.err) // Acquire load
	if p == nil {
		return nil
	}
	return *(*error)(p)
}

// Set sets an error. If called multiple times, only the first error is
// remembered.
func (e *T) Set(err error) {
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
