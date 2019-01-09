// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build !amd64 appengine

package simd

// FirstUnequal8Unsafe scans arg1[startPos:] and arg2[startPos:] for the first
// mismatching byte, returning its position if one is found, or the common
// length if all bytes match (or startPos >= len).  This has essentially the
// same speed as bytes.Compare().
//
// WARNING: This is a function designed to be used in inner loops, which makes
// assumptions about length and capacity which aren't checked at runtime.  Use
// the safe version of this function when that's a problem.
// The second assumption is always satisfied when the last
// potentially-size-increasing operation on arg1[] is {Re}makeUnsafe(),
// ResizeUnsafe(), or XcapUnsafe(), and the same is true for arg2[].
//
// 1. len(arg1) == len(arg2).
//
// 2. Capacities are at least RoundUpPow2(len, bytesPerVec).
func FirstUnequal8Unsafe(arg1, arg2 []byte, startPos int) int {
	endPos := len(arg1)
	for i := startPos; i < endPos; i++ {
		if arg1[i] != arg2[i] {
			return i
		}
	}
	return endPos
}

// FirstUnequal8 scans arg1[startPos:] and arg2[startPos:] for the first
// mismatching byte, returning its position if one is found, or the common
// length if all bytes match (or startPos >= len).  It panics if the lengths
// are not identical, or startPos is negative.
//
// This is essentially an extension of bytes.Compare().
func FirstUnequal8(arg1, arg2 []byte, startPos int) int {
	endPos := len(arg1)
	if endPos != len(arg2) {
		panic("FirstUnequal8() requires len(arg1) == len(arg2).")
	}
	if startPos < 0 {
		panic("FirstUnequal8() requires nonnegative startPos.")
	}
	for pos := startPos; pos < endPos; pos++ {
		if arg1[pos] != arg2[pos] {
			return pos
		}
	}
	return endPos
}

// FirstGreater8Unsafe scans arg[startPos:] for the first value larger than the
// given constant, returning its position if one is found, or len(arg) if all
// bytes are <= (or startPos >= len).
//
// This should only be used when greater values are usually present at ~5% or
// lower frequency.  Above that, use a simple for loop.
//
// WARNING: This is a function designed to be used in inner loops, which makes
// assumptions about length and capacity which aren't checked at runtime.  Use
// the safe version of this function when that's a problem.
// The second assumption is always satisfied when the last
// potentially-size-increasing operation on arg[] is {Re}makeUnsafe(),
// ResizeUnsafe(), or XcapUnsafe().
//
// 1. startPos is nonnegative.
//
// 2. cap(arg) >= RoundUpPow2(len, bytesPerVec).
func FirstGreater8Unsafe(arg []byte, val byte, startPos int) int {
	endPos := len(arg)
	for pos := startPos; pos < endPos; pos++ {
		if arg[pos] > val {
			return pos
		}
	}
	return endPos
}

// FirstGreater8 scans arg[startPos:] for the first value larger than the given
// constant, returning its position if one is found, or len(arg) if all bytes
// are <= (or startPos >= len).
//
// This should only be used when greater values are usually present at ~5% or
// lower frequency.  Above that, use a simple for loop.
func FirstGreater8(arg []byte, val byte, startPos int) int {
	if startPos < 0 {
		panic("FirstGreater8() requires nonnegative startPos.")
	}
	endPos := len(arg)
	for pos := startPos; pos < endPos; pos++ {
		if arg[pos] > val {
			return pos
		}
	}
	return endPos
}

// FirstLeq8Unsafe scans arg[startPos:] for the first value <= the given
// constant, returning its position if one is found, or len(arg) if all bytes
// are greater (or startPos >= len).
//
// This should only be used when <= values are usually present at ~5% or
// lower frequency.  Above that, use a simple for loop.
//
// See warning for FirstGreater8Unsafe.
func FirstLeq8Unsafe(arg []byte, val byte, startPos int) int {
	endPos := len(arg)
	for pos := startPos; pos < endPos; pos++ {
		if arg[pos] <= val {
			return pos
		}
	}
	return endPos
}

// FirstLeq8 scans arg[startPos:] for the first value <= the given constant,
// returning its position if one is found, or len(arg) if all bytes are greater
// (or startPos >= len).
//
// This should only be used when <= values are usually present at ~5% or lower
// frequency.  Above that, use a simple for loop.
func FirstLeq8(arg []byte, val byte, startPos int) int {
	// This currently has practically no performance penalty relative to the
	// Unsafe version, since the implementation is identical except for the
	// startPos check.
	if startPos < 0 {
		panic("FirstLeq8() requires nonnegative startPos.")
	}
	endPos := len(arg)
	for pos := startPos; pos < endPos; pos++ {
		if arg[pos] <= val {
			return pos
		}
	}
	return endPos
}
