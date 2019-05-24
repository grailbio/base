// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package writehash provides a set of utility functions to hash
// common types into hashes.
package writehash

import (
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math"
)

func must(n int, err error) {
	if err != nil {
		panic(fmt.Sprintf("writehash: hash.Write returned unexpected error: %v", err))
	}
}

// String encodes the string s into writer w.
func String(h hash.Hash, s string) {
	must(io.WriteString(h, s))
}

// Int encodes the integer v into writer w.
func Int(h hash.Hash, v int) {
	Uint64(h, uint64(v))
}

// Int16 encodes the 16-bit integer v into writer w.
func Int16(h hash.Hash, v int) {
	Uint16(h, uint16(v))
}

// Int32 encodes the 32-bit integer v into writer w.
func Int32(h hash.Hash, v int32) {
	Uint32(h, uint32(v))
}

// Int64 encodes the 64-bit integer v into writer w.
func Int64(h hash.Hash, v int64) {
	Uint64(h, uint64(v))
}

// Uint encodes the unsigned integer v into writer w.
func Uint(h hash.Hash, v uint) {
	Uint64(h, uint64(v))
}

// Uint16 encodes the unsigned 16-bit integer v into writer w.
func Uint16(h hash.Hash, v uint16) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	must(h.Write(buf[:]))
}

// Uint32 encodes the unsigned 32-bit integer v into writer w.
func Uint32(h hash.Hash, v uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	must(h.Write(buf[:]))
}

// Uint64 encodes the unsigned 64-bit integer v into writer w.
func Uint64(h hash.Hash, v uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	must(h.Write(buf[:]))
}

// Float32 encodes the 32-bit floating point number v into writer w.
func Float32(h hash.Hash, v float32) {
	Uint32(h, math.Float32bits(v))
}

// Float64 encodes the 64-bit floating point number v into writer w.
func Float64(h hash.Hash, v float64) {
	Uint64(h, math.Float64bits(v))
}

// Bool encodes the boolean v into writer w.
func Bool(h hash.Hash, v bool) {
	if v {
		Byte(h, 1)
	} else {
		Byte(h, 0)
	}
}

// Byte writes the byte b into writer w.
func Byte(h hash.Hash, b byte) {
	if w, ok := h.(io.ByteWriter); ok {
		must(0, w.WriteByte(b))
		return
	}
	must(h.Write([]byte{b}))
}

// Run encodes the rune r into writer w.
func Rune(h hash.Hash, r rune) {
	Int(h, int(r))
}
