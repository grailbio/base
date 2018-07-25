// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build amd64,!appengine

package simd

import (
	"reflect"
	"unsafe"
)

// *** the following functions are defined in add_amd64.s

//go:noescape
func addConst8TinyInplaceSSSE3Asm(main unsafe.Pointer, val int)

//go:noescape
func addConst8OddInplaceSSSE3Asm(main unsafe.Pointer, val, nByte int)

//go:noescape
func addConst8SSSE3Asm(dst, src unsafe.Pointer, val, nByte int)

//go:noescape
func addConst8OddSSSE3Asm(dst, src unsafe.Pointer, val, nByte int)

//go:noescape
func subtractFromConst8TinyInplaceSSSE3Asm(main unsafe.Pointer, val int)

//go:noescape
func subtractFromConst8OddInplaceSSSE3Asm(main unsafe.Pointer, val, nByte int)

//go:noescape
func subtractFromConst8SSSE3Asm(dst, src unsafe.Pointer, val, nByte int)

//go:noescape
func subtractFromConst8OddSSSE3Asm(dst, src unsafe.Pointer, val, nByte int)

// *** end assembly function signature(s)

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
	// Note that the word-based algorithm doesn't work so well here, since we'd
	// need to guard against bytes in the middle overflowing and polluting
	// adjacent bytes.
	mainLen := len(main)
	mainHeader := (*reflect.SliceHeader)(unsafe.Pointer(&main))
	if mainLen <= 16 {
		addConst8TinyInplaceSSSE3Asm(unsafe.Pointer(mainHeader.Data), int(val))
		return
	}
	addConst8OddInplaceSSSE3Asm(unsafe.Pointer(mainHeader.Data), int(val), mainLen)
}

// AddConst8Inplace adds the given constant to every byte of main[], with
// unsigned overflow.
func AddConst8Inplace(main []byte, val byte) {
	mainLen := len(main)
	if mainLen < 16 {
		for pos, mainByte := range main {
			main[pos] = val + mainByte
		}
		return
	}
	mainHeader := (*reflect.SliceHeader)(unsafe.Pointer(&main))
	addConst8OddInplaceSSSE3Asm(unsafe.Pointer(mainHeader.Data), int(val), mainLen)
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
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	addConst8SSSE3Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), int(val), srcHeader.Len)
}

// AddConst8 sets dst[pos] := src[pos] + val for every byte in src (with the
// usual unsigned overflow).  It panics if len(src) != len(dst).
func AddConst8(dst, src []byte, val byte) {
	srcLen := len(src)
	if len(dst) != srcLen {
		panic("AddConst8() requires len(src) == len(dst).")
	}
	if srcLen < 16 {
		for pos, curByte := range src {
			dst[pos] = curByte + val
		}
		return
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	addConst8OddSSSE3Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), int(val), srcLen)
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
	mainLen := len(main)
	mainHeader := (*reflect.SliceHeader)(unsafe.Pointer(&main))
	if mainLen <= 16 {
		subtractFromConst8TinyInplaceSSSE3Asm(unsafe.Pointer(mainHeader.Data), int(val))
		return
	}
	subtractFromConst8OddInplaceSSSE3Asm(unsafe.Pointer(mainHeader.Data), int(val), mainLen)
}

// SubtractFromConst8Inplace subtracts every byte of main[] from the given
// constant, with unsigned underflow.
func SubtractFromConst8Inplace(main []byte, val byte) {
	mainLen := len(main)
	if mainLen < 16 {
		for pos, mainByte := range main {
			main[pos] = val - mainByte
		}
		return
	}
	mainHeader := (*reflect.SliceHeader)(unsafe.Pointer(&main))
	subtractFromConst8OddInplaceSSSE3Asm(unsafe.Pointer(mainHeader.Data), int(val), mainLen)
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
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	subtractFromConst8SSSE3Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), int(val), srcHeader.Len)
}

// SubtractFromConst8 sets dst[pos] := val - src[pos] for every byte in src
// (with the usual unsigned overflow).  It panics if len(src) != len(dst).
func SubtractFromConst8(dst, src []byte, val byte) {
	srcLen := len(src)
	if len(dst) != srcLen {
		panic("SubtractFromConst8() requires len(src) == len(dst).")
	}
	if srcLen < 16 {
		for pos, curByte := range src {
			dst[pos] = val - curByte
		}
		return
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	subtractFromConst8OddSSSE3Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), int(val), srcLen)
}
