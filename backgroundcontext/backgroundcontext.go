// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package backgroundcontext manages the singleton v23 context.  This
// package is not for general use. It it only for packages that (1)
// need to access a background context, and (2) are used both as a
// binary and as a shared library (e.g., in R).
package backgroundcontext

import (
	"context"
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

// Get returns a background context: if a v23 context has been set, it is returned;
// otherwise the standard Go background context is returned.
func Get() context.Context {
	if p := (*v23context.T)(atomic.LoadPointer(&ptr)); p != nil {
		return p
	}
	return context.Background()
}

type wrapped struct {
	context.Context
	v23ctx *v23context.T
}

func (w *wrapped) Value(key interface{}) interface{} {
	val := w.Context.Value(key)
	if val != nil {
		return val
	}
	return w.v23ctx.Value(key)
}

// Wrap wraps the provided context, composing it with the defined
// background context, if any. This allows contexts to be wrapped
// so that v23 stubs can get the background context's RPC client.
//
// Cancelations are not forwarded: it is assumed that the set background
// context is never canceled.
//
// BUG: this is very complicated; but it seems required to make
// vanadium play nicely with contexts defined outside of its
// universe.
func Wrap(ctx context.Context) context.Context {
	v23ctx := (*v23context.T)(atomic.LoadPointer(&ptr))
	if v23ctx == nil {
		return ctx
	}
	return &wrapped{ctx, v23ctx}
}
