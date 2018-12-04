// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package errors_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/grailbio/base/errors"
	"github.com/stretchr/testify/require"
)

func TestOnce(t *testing.T) {
	e := errors.Once{}
	require.NoError(t, e.Err())

	e.Set(errors.New("testerror"))
	require.EqualError(t, e.Err(), "testerror")
	e.Set(errors.New("testerror2")) // ignored
	require.EqualError(t, e.Err(), "testerror")
	runtime.GC()
	require.EqualError(t, e.Err(), "testerror")
}

func BenchmarkReadNoError(b *testing.B) {
	e := errors.Once{}
	for i := 0; i < b.N; i++ {
		if e.Err() != nil {
			require.Fail(b, "err")
		}
	}
}

func BenchmarkReadError(b *testing.B) {
	e := errors.Once{}
	e.Set(errors.New("testerror"))
	for i := 0; i < b.N; i++ {
		if e.Err() == nil {
			require.Fail(b, "err")
		}
	}
}

func BenchmarkSet(b *testing.B) {
	e := errors.Once{}
	err := errors.New("testerror")
	for i := 0; i < b.N; i++ {
		e.Set(err)
	}
}

func ExampleErrorReporter() {
	e := errors.Once{}
	fmt.Printf("Error: %v\n", e.Err())
	e.Set(errors.New("test error 0"))
	fmt.Printf("Error: %v\n", e.Err())
	e.Set(errors.New("test error 1"))
	fmt.Printf("Error: %v\n", e.Err())
	// Output:
	// Error: <nil>
	// Error: test error 0
	// Error: test error 0
}
