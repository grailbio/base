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

// Func is the function called to report an error and interrupt
// execution. Func is typically set to log.Panic or log.Fatal. It
// should be set before any potential calls to functions in the
// must package.
var Func func(...interface{}) = log.Panic

// Nil asserts that v is nil; v is typically a value of type error.
// If v is not nil, Nil formats a message in hte manner of fmt.Sprint
// and calls must.Func. Nil also suffixes the message with the
// fmt.Sprint-formatted value of v.
func Nil(v interface{}, args ...interface{}) {
	if v == nil {
		return
	}
	if len(args) == 0 {
		Func(v)
		return
	}
	Func(fmt.Sprint(args...), ": ", v)
}

// Nilf asserts that v is nil; v is typically a value of type error.
// If v is not nil, Nilf formats a message in hte manner of
// fmt.Sprintf and calls must.Func. Nilf also suffixes the message
// with the fmt.Sprint-formatted value of v.
func Nilf(v interface{}, format string, args ...interface{}) {
	if v == nil {
		return
	}
	Func(fmt.Sprintf(format, args...), ": ", v)
}

// True is a no-op if the value b is true. If it is false, True
// formats a message in the manner of fmt.Sprint and calls Func.
func True(b bool, v ...interface{}) {
	if b {
		return
	}
	if len(v) == 0 {
		Func("must: assertion failed")
		return
	}
	Func(v...)
}

// Truef is a no-op if the value b is true. If it is false, True
// formats a message in the manner of fmt.Sprintf and calls Func.
func Truef(x bool, format string, v ...interface{}) {
	if x {
		return
	}
	Func(fmt.Sprintf(format, v...))
}

// Never asserts that it is never called. If it is, it formats a message
// in the manner of fmt.Sprint and calls Func.
func Never(v ...interface{}) {
	Func(v...)
}

// Neverf asserts that it is never called. If it is, it formats a message
// in the manner of fmt.Sprintf and calls Func.
func Neverf(format string, v ...interface{}) {
	Func(fmt.Sprintf(format, v...))
}
