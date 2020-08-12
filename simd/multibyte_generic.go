// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build !amd64,!appengine

package simd

import (
	"reflect"
	"unsafe"
)

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

// Memset16Raw assumes dst points to an array of nElem 2-byte elements, and
// valPtr points to a single 2-byte element.  It fills dst with copies of
// *valPtr.
func Memset16Raw(dst, valPtr unsafe.Pointer, nElem int) {
	val := *((*uint16)(valPtr))
	for idx := 0; idx != nElem; idx++ {
		*((*uint16)(dst)) = val
		dst = unsafe.Pointer(uintptr(dst) + 2)
	}
}

// Memset32Raw assumes dst points to an array of nElem 4-byte elements, and
// valPtr points to a single 4-byte element.  It fills dst with copies of
// *valPtr.
func Memset32Raw(dst, valPtr unsafe.Pointer, nElem int) {
	val := *((*uint32)(valPtr))
	for idx := 0; idx != nElem; idx++ {
		*((*uint32)(dst)) = val
		dst = unsafe.Pointer(uintptr(dst) + 4)
	}
}

// RepeatI16 fills dst[] with the given int16.
func RepeatI16(dst []int16, val int16) {
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	Memset16Raw(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(&val), dstHeader.Len)
}

// RepeatU16 fills dst[] with the given uint16.
func RepeatU16(dst []uint16, val uint16) {
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	Memset16Raw(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(&val), dstHeader.Len)
}

// IndexU16 returns the index of the first instance of val in main, or -1 if
// val is not present in main.
func IndexU16(main []uint16, val uint16) int {
	for i, v := range main {
		if v == val {
			return i
		}
	}
	return -1
}

// (Add a function which has the original little-endian byte-slice semantics if
// we ever need it.)

// Reverse16InplaceRaw assumes main points to an array of ct 2-byte elements,
// and reverses it in-place.
func Reverse16InplaceRaw(main unsafe.Pointer, nElem int) {
	nElemDiv2 := nElem >> 1
	fwdIter := main
	revIter := unsafe.Pointer(uintptr(main) + uintptr((nElem-1)*2))
	for idx := 0; idx != nElemDiv2; idx++ {
		origLeftVal := *((*uint16)(fwdIter))
		*((*uint16)(fwdIter)) = *((*uint16)(revIter))
		*((*uint16)(revIter)) = origLeftVal
		fwdIter = unsafe.Pointer(uintptr(fwdIter) + 2)
		revIter = unsafe.Pointer(uintptr(revIter) - 2)
	}
}

// Reverse16Raw assumes dst and src both point to arrays of ct 2-byte elements,
// and sets dst[pos] := src[ct - 1 - pos] for each position.
func Reverse16Raw(dst, src unsafe.Pointer, nElem int) {
	srcIter := unsafe.Pointer(uintptr(src) + uintptr((nElem-1)*2))
	dstIter := dst
	for idx := 0; idx != nElem; idx++ {
		*((*uint16)(dstIter)) = *((*uint16)(srcIter))
		srcIter = unsafe.Pointer(uintptr(srcIter) - 2)
		dstIter = unsafe.Pointer(uintptr(dstIter) + 2)
	}
}

// ReverseI16Inplace reverses a []int16 in-place.
func ReverseI16Inplace(main []int16) {
	mainHeader := (*reflect.SliceHeader)(unsafe.Pointer(&main))
	Reverse16InplaceRaw(unsafe.Pointer(mainHeader.Data), mainHeader.Len)
}

// ReverseU16Inplace reverses a []uint16 in-place.
func ReverseU16Inplace(main []uint16) {
	mainHeader := (*reflect.SliceHeader)(unsafe.Pointer(&main))
	Reverse16InplaceRaw(unsafe.Pointer(mainHeader.Data), mainHeader.Len)
}

// ReverseI16 sets dst[len(src) - 1 - pos] := src[pos] for each position in
// src.  It panics if len(src) != len(dst).
func ReverseI16(dst, src []int16) {
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	nElem := srcHeader.Len
	if nElem != dstHeader.Len {
		panic("ReverseI16() requires len(src) == len(dst).")
	}
	Reverse16Raw(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), nElem)
}

// ReverseU16 sets dst[len(src) - 1 - pos] := src[pos] for each position in
// src.  It panics if len(src) != len(dst).
func ReverseU16(dst, src []uint16) {
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	nElem := srcHeader.Len
	if nElem != dstHeader.Len {
		panic("ReverseU16() requires len(src) == len(dst).")
	}
	Reverse16Raw(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), nElem)
}

// Benchmark results suggest that Reverse32Raw is unimportant.
