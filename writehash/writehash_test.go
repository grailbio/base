// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package writehash_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/grailbio/base/writehash"
)

type fakeHasher struct{ io.Writer }

func (fakeHasher) Sum([]byte) []byte { panic("sum") }
func (fakeHasher) Reset()            { panic("reset") }
func (fakeHasher) Size() int         { panic("size") }
func (fakeHasher) BlockSize() int    { panic("blocksize") }

func TestWritehash(t *testing.T) {
	b := new(bytes.Buffer)
	var lastLen int
	check := func(n int) {
		t.Helper()
		if got, want := b.Len()-lastLen, n; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		if bytes.Equal(b.Bytes()[lastLen:], make([]byte, n)) {
			t.Error("wrote zeros")
		}
		lastLen = b.Len()
	}
	h := fakeHasher{b}
	writehash.String(h, "hello world")
	check(11)
	writehash.Int(h, 1)
	check(8)
	writehash.Int16(h, 1)
	check(2)
	writehash.Int32(h, 1)
	check(4)
	writehash.Int64(h, 1)
	check(8)
	writehash.Uint(h, 1)
	check(8)
	writehash.Uint16(h, 1)
	check(2)
	writehash.Uint32(h, 1)
	check(4)
	writehash.Float32(h, 1)
	check(4)
	writehash.Float64(h, 1)
	check(8)
	writehash.Bool(h, true)
	check(1)
	writehash.Byte(h, 1)
	check(1)
	writehash.Rune(h, 'x')
	check(8)
}
