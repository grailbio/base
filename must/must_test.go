// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package must_test

import (
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/grailbio/base/must"
)

// TestDepth verifies that the depth passed to Func correctly locates the
// caller of the must function.
func TestDepth(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine current file")
	}
	must.Func = func(depth int, v ...interface{}) {
		_, file, _, ok := runtime.Caller(depth)
		if !ok {
			t.Fatal("could not determine caller of Func")
		}
		if file != thisFile {
			t.Errorf("caller at depth %d is '%s'; should be '%s'", depth, file, thisFile)
		}
	}
	must.True(false)
	must.Truef(false, "")
	must.Nil(struct{}{})
	must.Nilf(struct{}{}, "")
	must.Never()
	must.Neverf("")
}

func Example() {
	must.Func = func(depth int, v ...interface{}) {
		fmt.Print(v...)
		fmt.Print("\n")
	}

	must.Nil(errors.New("unexpected condition"))
	must.Nil(nil)
	must.Nil(errors.New("some error"))
	must.Nil(errors.New("i/o error"), "reading file")

	must.True(false)
	must.True(true, "something happened")
	must.True(false, "a condition failed")

	// Output:
	// unexpected condition
	// some error
	// reading file: i/o error
	// must: assertion failed
	// a condition failed
}
