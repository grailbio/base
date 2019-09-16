// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package must_test

import (
	"errors"
	"fmt"

	"github.com/grailbio/base/must"
)

func ExampleMust() {
	must.Func = func(v ...interface{}) { fmt.Print(v...); fmt.Print("\n") }

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
