// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build appengine

package simd

// This file contains functions which operate on slices of 2- or 4-byte
// elements (typically small structs or integers) in ways that differ from the
// corresponding operations on single-byte elements.
// In this context, there is little point in making the interface based on
// []byte, since the caller will need to unsafely cast to it.  Instead, most
// functions take unsafe.Pointer(s) and a count, and have names ending in
// 'Raw'; the caller should write safe wrappers around them when appropriate.
// We provide sample wrappers for the int16 and uint16 cases.  (Originally did
// this for int32/uint32, but turns out the compiler has hardcoded
// optimizations for those cases which are currently missing for {u}int16.)

// RepeatI16 fills dst[] with the given int16.
func RepeatI16(dst []int16, val int16) {
	for i := range dst {
		dst[i] = val
	}
}

// RepeatU16 fills dst[] with the given uint16.
func RepeatU16(dst []uint16, val uint16) {
	for i := range dst {
		dst[i] = val
	}
}

// ReverseI16Inplace reverses a []int16 in-place.
func ReverseI16Inplace(main []int16) {
	nElem := len(main)
	nElemDiv2 := nElem >> 1
	for i, j := 0, nElem-1; i != nElemDiv2; i, j = i+1, j-1 {
		main[i], main[j] = main[j], main[i]
	}
}

// ReverseU16Inplace reverses a []uint16 in-place.
func ReverseU16Inplace(main []uint16) {
	nElem := len(main)
	nElemDiv2 := nElem >> 1
	for i, j := 0, nElem-1; i != nElemDiv2; i, j = i+1, j-1 {
		main[i], main[j] = main[j], main[i]
	}
}

// ReverseI16 sets dst[len(src) - 1 - pos] := src[pos] for each position in
// src.  It panics if len(src) != len(dst).
func ReverseI16(dst, src []int16) {
	if len(dst) != len(src) {
		panic("ReverseI16() requires len(src) == len(dst).")
	}
	nElemMinus1 := len(dst) - 1
	for i := range dst {
		dst[i] = src[nElemMinus1-i]
	}
}

// ReverseU16 sets dst[len(src) - 1 - pos] := src[pos] for each position in
// src.  It panics if len(src) != len(dst).
func ReverseU16(dst, src []uint16) {
	if len(dst) != len(src) {
		panic("ReverseU16() requires len(src) == len(dst).")
	}
	nElemMinus1 := len(dst) - 1
	for i := range dst {
		dst[i] = src[nElemMinus1-i]
	}
}

// Benchmark results suggest that Reverse32Raw is unimportant.
