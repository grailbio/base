// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package backgroundcontext manages the singleton v23 context.  This package is
// not for general use. It it only for packages that (1) need to access a
// background context, and (2) are used both as a binary and as a shared library
// (e.g., in R).
package backgroundcontext

import (
	"sync/atomic"
	"unsafe"

	v23context "v.io/v23/context"
)

var ptr unsafe.Pointer

// Set sets the singleton global context. It should be called at most once,
// usually immediately after a process starts. Calling Set() multiple times will
// cause a panic. Thread safe.
func Set(ctx *v23context.T) {
	if !atomic.CompareAndSwapPointer(&ptr, nil, unsafe.Pointer(ctx)) {
		panic("backgroundcontext.Set called twice")
	}
}

// T returns the background context set by Set. It panics if Set has not been
// called yet. Thread safe.
func T() *v23context.T {
	p := atomic.LoadPointer(&ptr)
	if p == nil {
		panic("backgroundcontext.Set not yet called")
	}
	return (*v23context.T)(p)
}
