// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package must provides a handful of functions to express fatal
// assertions in Go programs. It is meant to alleviate cumbersome
// error handling and reporting when the only course of action is to
// fail the program. Package must is intended to be used by top-level
// binaries (i.e., in main packages); it should rarely be used
// elsewhere.
package must

import (
	"fmt"

	"github.com/grailbio/base/log"
)

// Func is the function called to report an error and interrupt execution. Func
// is typically set to a function that logs the message and halts execution,
// e.g. by panicking. It should be set before any potential calls to functions
// in the must package. Func is passed the call depth of the caller of the must
// function, e.g. the caller of Nil. This can be used to annotate messages.
//
// The default implementation logs the message with
// github.com/grailbio/base/log at the Error level and then panics.
var Func func(int, ...interface{}) = func(depth int, v ...interface{}) {
	s := fmt.Sprint(v...)
	// Nothing to do if output fails.
	_ = log.Output(depth+1, log.Error, s)
	panic(s)
}

// Nil asserts that v is nil; v is typically a value of type error.
// If v is not nil, Nil formats a message in the manner of fmt.Sprint
// and calls must.Func. Nil also suffixes the message with the
// fmt.Sprint-formatted value of v.
func Nil(v interface{}, args ...interface{}) {
	if v == nil {
		return
	}
	if len(args) == 0 {
		Func(2, v)
		return
	}
	Func(2, fmt.Sprint(args...), ": ", v)
}

// Nilf asserts that v is nil; v is typically a value of type error.
// If v is not nil, Nilf formats a message in the manner of
// fmt.Sprintf and calls must.Func. Nilf also suffixes the message
// with the fmt.Sprint-formatted value of v.
func Nilf(v interface{}, format string, args ...interface{}) {
	if v == nil {
		return
	}
	Func(2, fmt.Sprintf(format, args...), ": ", v)
}

// True is a no-op if the value b is true. If it is false, True
// formats a message in the manner of fmt.Sprint and calls Func.
func True(b bool, v ...interface{}) {
	if b {
		return
	}
	if len(v) == 0 {
		Func(2, "must: assertion failed")
		return
	}
	Func(2, v...)
}

// Truef is a no-op if the value b is true. If it is false, True
// formats a message in the manner of fmt.Sprintf and calls Func.
func Truef(x bool, format string, v ...interface{}) {
	if x {
		return
	}
	Func(2, fmt.Sprintf(format, v...))
}

// Never asserts that it is never called. If it is, it formats a message
// in the manner of fmt.Sprint and calls Func.
func Never(v ...interface{}) {
	Func(2, v...)
}

// Neverf asserts that it is never called. If it is, it formats a message
// in the manner of fmt.Sprintf and calls Func.
func Neverf(format string, v ...interface{}) {
	Func(2, fmt.Sprintf(format, v...))
}
