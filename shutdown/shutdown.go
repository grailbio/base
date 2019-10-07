// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package shutdown implements a global process shutdown mechanism.
// It is used by package github.com/grailbio/base/grail to perform
// graceful shutdown of software components. It is a separate package
// in order to avoid circular dependencies.
package shutdown

import "sync"

// Func is the type of function run on shutdowns.
type Func func()

var (
	mu    sync.Mutex
	funcs []Func
)

// Register registers a function to be run in the Init shutdown
// callback. The callbacks will run in the reverse order of
// registration.

func Register(f Func) {
	mu.Lock()
	funcs = append(funcs, f)
	mu.Unlock()
}

// Run run callbacks added by Register. This function is not for
// general use.
func Run() {
	mu.Lock()
	fns := funcs
	funcs = nil
	mu.Unlock()
	for i := len(fns) - 1; i >= 0; i-- {
		fns[i]()
	}
}
