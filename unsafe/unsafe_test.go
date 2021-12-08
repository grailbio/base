// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package unsafe_test

import (
	"fmt"
	"testing"

	"github.com/grailbio/base/unsafe"
)

func TestBytesToString(t *testing.T) {
	for _, src := range []string{"", "abc"} {
		d := unsafe.BytesToString([]byte(src))
		if d != src {
			t.Error(d)
		}
	}
}

func ExampleBytesToString() {
	fmt.Println(unsafe.BytesToString([]byte{'A', 'b', 'C'}))
	// Output: AbC
}

func TestStringToBytes(t *testing.T) {
	for _, src := range []string{"", "abc"} {
		d := unsafe.StringToBytes(src)
		if string(d) != src {
			t.Error(d)
		}
	}
}

func ExampleStringToBytes() {
	fmt.Println(unsafe.StringToBytes("AbC"))
	// Output: [65 98 67]
}
