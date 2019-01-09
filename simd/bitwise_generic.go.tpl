// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build !amd64 appengine

package PACKAGE

// ZZUnsafeInplace sets main[pos] := main[pos] OPCHAR arg[pos] for every position
// in main[].
//
// WARNING: This is a function designed to be used in inner loops, which makes
// assumptions about length and capacity which aren't checked at runtime.  Use
// the safe version of this function when that's a problem.
// Assumptions #2-3 are always satisfied when the last
// potentially-size-increasing operation on arg[] is {Re}makeUnsafe(),
// ResizeUnsafe(), or XcapUnsafe(), and the same is true for main[].
//
// 1. len(arg) and len(main) must be equal.
//
// 2. Capacities are at least RoundUpPow2(len(main) + 1, bytesPerVec).
//
// 3. The caller does not care if a few bytes past the end of main[] are
// changed.
func ZZUnsafeInplace(main, arg []byte) {
	for i, x := range main {
		main[i] = x OPCHAR arg[i]
	}
}

// ZZInplace sets main[pos] := main[pos] OPCHAR arg[pos] for every position in
// main[].  It panics if slice lengths don't match.
func ZZInplace(main, arg []byte) {
	if len(arg) != len(main) {
		panic("ZZInplace() requires len(arg) == len(main).")
	}
	for i, x := range main {
		main[i] = x OPCHAR arg[i]
	}
}

// ZZUnsafe sets dst[pos] := src1[pos] OPCHAR src2[pos] for every position in dst.
//
// WARNING: This is a function designed to be used in inner loops, which makes
// assumptions about length and capacity which aren't checked at runtime.  Use
// the safe version of this function when that's a problem.
// Assumptions #2-3 are always satisfied when the last
// potentially-size-increasing operation on src1[] is {Re}makeUnsafe(),
// ResizeUnsafe(), or XcapUnsafe(), and the same is true for src2[] and dst[].
//
// 1. len(src1), len(src2), and len(dst) must be equal.
//
// 2. Capacities are at least RoundUpPow2(len(dst) + 1, bytesPerVec).
//
// 3. The caller does not care if a few bytes past the end of dst[] are
// changed.
func ZZUnsafe(dst, src1, src2 []byte) {
	for i, x := range src1 {
		dst[i] = x OPCHAR src2[i]
	}
}

// ZZ sets dst[pos] := src1[pos] OPCHAR src2[pos] for every position in dst.  It
// panics if slice lengths don't match.
func ZZ(dst, src1, src2 []byte) {
	dstLen := len(dst)
	if (len(src1) != dstLen) || (len(src2) != dstLen) {
		panic("ZZ() requires len(src1) == len(src2) == len(dst).")
	}
	for i, x := range src1 {
		dst[i] = x OPCHAR src2[i]
	}
}

// ZZConst8UnsafeInplace sets main[pos] := main[pos] OPCHAR val for every position
// in main[].
//
// WARNING: This is a function designed to be used in inner loops, which makes
// assumptions about length and capacity which aren't checked at runtime.  Use
// the safe version of this function when that's a problem.
// These assumptions are always satisfied when the last
// potentially-size-increasing operation on main[] is {Re}makeUnsafe(),
// ResizeUnsafe(), or XcapUnsafe().
//
// 1. cap(main) is at least RoundUpPow2(len(main) + 1, bytesPerVec).
//
// 2. The caller does not care if a few bytes past the end of main[] are
// changed.
func ZZConst8UnsafeInplace(main []byte, val byte) {
	for i, x := range main {
		main[i] = x OPCHAR val
	}
}

// ZZConst8Inplace sets main[pos] := main[pos] OPCHAR val for every position in
// main[].
func ZZConst8Inplace(main []byte, val byte) {
	for i, x := range main {
		main[i] = x OPCHAR val
	}
}

// ZZConst8Unsafe sets dst[pos] := src[pos] OPCHAR val for every position in dst.
//
// WARNING: This is a function designed to be used in inner loops, which makes
// assumptions about length and capacity which aren't checked at runtime.  Use
// the safe version of this function when that's a problem.
// Assumptions #2-3 are always satisfied when the last
// potentially-size-increasing operation on src[] is {Re}makeUnsafe(),
// ResizeUnsafe(), or XcapUnsafe(), and the same is true for dst[].
//
// 1. len(src) and len(dst) must be equal.
//
// 2. Capacities are at least RoundUpPow2(len(dst) + 1, bytesPerVec).
//
// 3. The caller does not care if a few bytes past the end of dst[] are
// changed.
func ZZConst8Unsafe(dst, src []byte, val byte) {
	for i, x := range src {
		dst[i] = x OPCHAR val
	}
}

// ZZConst8 sets dst[pos] := src[pos] OPCHAR val for every position in dst.  It
// panics if slice lengths don't match.
func ZZConst8(dst, src []byte, val byte) {
	if len(src) != len(dst) {
		panic("ZZConst8() requires len(src) == len(dst).")
	}
	for i, x := range src {
		dst[i] = x OPCHAR val
	}
}
