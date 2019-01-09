// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build !amd64 appengine

package simd

// AddConst8UnsafeInplace adds the given constant to every byte of main[], with
// unsigned overflow.
//
// WARNING: This is a function designed to be used in inner loops, which
// assumes without checking that capacity is at least RoundUpPow2(len(main),
// bytesPerVec).  It also assumes that the caller does not care if a few bytes
// past the end of main[] are changed.  Use the safe version of this function
// if any of these properties are problematic.
// These assumptions are always satisfied when the last
// potentially-size-increasing operation on main[] is {Re}makeUnsafe(),
// ResizeUnsafe(), or XcapUnsafe().
func AddConst8UnsafeInplace(main []byte, val byte) {
	for i, x := range main {
		main[i] = x + val
	}
}

// AddConst8Inplace adds the given constant to every byte of main[], with
// unsigned overflow.
func AddConst8Inplace(main []byte, val byte) {
	for i, x := range main {
		main[i] = x + val
	}
}

// AddConst8Unsafe sets dst[pos] := src[pos] + val for every byte in src (with
// the usual unsigned overflow).
//
// WARNING: This is a function designed to be used in inner loops, which makes
// assumptions about length and capacity which aren't checked at runtime.  Use
// the safe version of this function when that's a problem.
// Assumptions #2-3 are always satisfied when the last
// potentially-size-increasing operation on src[] is {Re}makeUnsafe(),
// ResizeUnsafe() or XcapUnsafe(), and the same is true for dst[].
//
// 1. len(src) and len(dst) are equal.
//
// 2. Capacities are at least RoundUpPow2(len(src) + 1, bytesPerVec).
//
// 3. The caller does not care if a few bytes past the end of dst[] are
// changed.
func AddConst8Unsafe(dst, src []byte, val byte) {
	for i, x := range src {
		dst[i] = x + val
	}
}

// AddConst8 sets dst[pos] := src[pos] + val for every byte in src (with the
// usual unsigned overflow).  It panics if len(src) != len(dst).
func AddConst8(dst, src []byte, val byte) {
	if len(dst) != len(src) {
		panic("AddConst8() requires len(src) == len(dst).")
	}
	for i, x := range src {
		dst[i] = x + val
	}
}

// SubtractFromConst8UnsafeInplace subtracts every byte of main[] from the
// given constant, with unsigned underflow.
//
// WARNING: This is a function designed to be used in inner loops, which
// assumes without checking that capacity is at least RoundUpPow2(len(main),
// bytesPerVec).  It also assumes that the caller does not care if a few bytes
// past the end of main[] are changed.  Use the safe version of this function
// if any of these properties are problematic.
// These assumptions are always satisfied when the last
// potentially-size-increasing operation on main[] is {Re}makeUnsafe(),
// ResizeUnsafe(), or XcapUnsafe().
func SubtractFromConst8UnsafeInplace(main []byte, val byte) {
	for i, x := range main {
		main[i] = val - x
	}
}

// SubtractFromConst8Inplace subtracts every byte of main[] from the given
// constant, with unsigned underflow.
func SubtractFromConst8Inplace(main []byte, val byte) {
	for i, x := range main {
		main[i] = val - x
	}
}

// SubtractFromConst8Unsafe sets dst[pos] := val - src[pos] for every byte in
// src (with the usual unsigned overflow).
//
// WARNING: This is a function designed to be used in inner loops, which makes
// assumptions about length and capacity which aren't checked at runtime.  Use
// the safe version of this function when that's a problem.
// Assumptions #2-3 are always satisfied when the last
// potentially-size-increasing operation on src[] is {Re}makeUnsafe(),
// ResizeUnsafe() or XcapUnsafe(), and the same is true for dst[].
//
// 1. len(src) and len(dst) are equal.
//
// 2. Capacities are at least RoundUpPow2(len(src) + 1, bytesPerVec).
//
// 3. The caller does not care if a few bytes past the end of dst[] are
// changed.
func SubtractFromConst8Unsafe(dst, src []byte, val byte) {
	for i, x := range src {
		dst[i] = val - x
	}
}

// SubtractFromConst8 sets dst[pos] := val - src[pos] for every byte in src
// (with the usual unsigned overflow).  It panics if len(src) != len(dst).
func SubtractFromConst8(dst, src []byte, val byte) {
	if len(dst) != len(src) {
		panic("SubtractFromConst8() requires len(src) == len(dst).")
	}
	for i, x := range src {
		dst[i] = val - x
	}
}
