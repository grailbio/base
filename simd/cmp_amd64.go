// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build amd64,!appengine

package simd

import (
	"math/bits"
	"reflect"
	"unsafe"
)

//go:noescape
func firstGreater8SSSE3Asm(arg unsafe.Pointer, val, startPos, endPos int) int

//go:noescape
func firstLeq8SSSE3Asm(arg unsafe.Pointer, val, startPos, endPos int) int

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
	// Possible alternative interface for these functions: fill a bitarray, with
	// set bits for each mismatching position.  Can return popcount.
	endPos := len(arg1)
	nByte := endPos - startPos
	if nByte <= 0 {
		return endPos
	}
	arg1Header := (*reflect.SliceHeader)(unsafe.Pointer(&arg1))
	arg2Header := (*reflect.SliceHeader)(unsafe.Pointer(&arg2))
	nWordMinus1 := (nByte - 1) >> Log2BytesPerWord
	arg1Iter := unsafe.Pointer(arg1Header.Data + uintptr(startPos))
	arg2Iter := unsafe.Pointer(arg2Header.Data + uintptr(startPos))
	// Tried replacing this with simple (non-unrolled) vector-based loops very
	// similar to the main runtime's go/src/internal/bytealg/compare_amd64.s, but
	// they were actually worse than the safe function on the short-array
	// benchmark.  Can eventually look at what clang/LLVM actually generates for
	// plink2_string.cc FirstUnequal4()--I confirmed that the word-based loop is
	// slower than the SSE2 vector-based loop there--but that can wait until AVX2
	// support is added.
	for widx := 0; widx < nWordMinus1; widx++ {
		xorWord := (*((*uintptr)(arg1Iter))) ^ (*((*uintptr)(arg2Iter)))
		if xorWord != 0 {
			// Unfortunately, in a primarily-signed-int codebase, ">> 3" should
			// generally be written over the more readable "/ 8", because the latter
			// requires additional code to handle negative numerators.  In this
			// specific case, I'd hope that the compiler is smart enough to prove
			// that bits.TrailingZeros64() returns nonnegative values, and would then
			// optimize "/ 8" appropriately, but it is better to not worry about the
			// matter at all.
			return startPos + (widx * BytesPerWord) + (bits.TrailingZeros64(uint64(xorWord)) >> 3)
		}
		arg1Iter = unsafe.Pointer(uintptr(arg1Iter) + BytesPerWord)
		arg2Iter = unsafe.Pointer(uintptr(arg2Iter) + BytesPerWord)
	}
	xorWord := (*((*uintptr)(arg1Iter))) ^ (*((*uintptr)(arg2Iter)))
	if xorWord == 0 {
		return endPos
	}
	unequalPos := startPos + nWordMinus1*BytesPerWord + (bits.TrailingZeros64(uint64(xorWord)) >> 3)
	if unequalPos > endPos {
		return endPos
	}
	return unequalPos
}

// FirstUnequal8 scans arg1[startPos:] and arg2[startPos:] for the first
// mismatching byte, returning its position if one is found, or the common
// length if all bytes match (or startPos >= len).  It panics if the lengths
// are not identical, or startPos is negative.
//
// This is essentially an extension of bytes.Compare().
func FirstUnequal8(arg1, arg2 []byte, startPos int) int {
	// This takes ~10% longer on the short-array benchmark.
	endPos := len(arg1)
	if endPos != len(arg2) || (startPos < 0) {
		// The startPos < 0 check is kind of paranoid.  It's here because
		// unsafe.Pointer(arg1Header.Data + uintptr(startPos)) does not
		// automatically error out on negative startPos, and it also doesn't hurt
		// to protect against (endPos - startPos) integer overflow; but feel free
		// to request its removal if you are using this function in a time-critical
		// loop.
		panic("FirstUnequal8() requires len(arg1) == len(arg2) and nonnegative startPos.")
	}
	nByte := endPos - startPos
	if nByte < BytesPerWord {
		for pos := startPos; pos < endPos; pos++ {
			if arg1[pos] != arg2[pos] {
				return pos
			}
		}
		return endPos
	}
	arg1Header := (*reflect.SliceHeader)(unsafe.Pointer(&arg1))
	arg2Header := (*reflect.SliceHeader)(unsafe.Pointer(&arg2))
	nWordMinus1 := (nByte - 1) >> Log2BytesPerWord
	arg1Iter := unsafe.Pointer(arg1Header.Data + uintptr(startPos))
	arg2Iter := unsafe.Pointer(arg2Header.Data + uintptr(startPos))
	for widx := 0; widx < nWordMinus1; widx++ {
		xorWord := (*((*uintptr)(arg1Iter))) ^ (*((*uintptr)(arg2Iter)))
		if xorWord != 0 {
			return startPos + (widx * BytesPerWord) + (bits.TrailingZeros64(uint64(xorWord)) >> 3)
		}
		arg1Iter = unsafe.Pointer(uintptr(arg1Iter) + BytesPerWord)
		arg2Iter = unsafe.Pointer(uintptr(arg2Iter) + BytesPerWord)
	}
	finalOffset := uintptr(endPos - BytesPerWord)
	arg1FinalPtr := unsafe.Pointer(arg1Header.Data + finalOffset)
	arg2FinalPtr := unsafe.Pointer(arg2Header.Data + finalOffset)
	xorWord := (*((*uintptr)(arg1FinalPtr))) ^ (*((*uintptr)(arg2FinalPtr)))
	if xorWord == 0 {
		return endPos
	}
	return int(finalOffset) + (bits.TrailingZeros64(uint64(xorWord)) >> 3)
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
	nByte := endPos - startPos
	if nByte <= bytesPerVec {
		// Main loop setup overhead is pretty high. Crossover point in benchmarks
		// is in the 16-32 byte range (depending on sparsity).
		for pos := startPos; pos < endPos; pos++ {
			if arg[pos] > val {
				return pos
			}
		}
		return endPos
	}
	argHeader := (*reflect.SliceHeader)(unsafe.Pointer(&arg))
	return firstGreater8SSSE3Asm(unsafe.Pointer(argHeader.Data), int(val), startPos, endPos)
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
	nByte := endPos - startPos
	if nByte <= bytesPerVec {
		for pos := startPos; pos < endPos; pos++ {
			if arg[pos] > val {
				return pos
			}
		}
		return endPos
	}
	argHeader := (*reflect.SliceHeader)(unsafe.Pointer(&arg))
	return firstGreater8SSSE3Asm(unsafe.Pointer(argHeader.Data), int(val), startPos, endPos)
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
	nByte := endPos - startPos
	if nByte <= bytesPerVec {
		for pos := startPos; pos < endPos; pos++ {
			if arg[pos] <= val {
				return pos
			}
		}
		return endPos
	}
	argHeader := (*reflect.SliceHeader)(unsafe.Pointer(&arg))
	return firstLeq8SSSE3Asm(unsafe.Pointer(argHeader.Data), int(val), startPos, endPos)
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
	nByte := endPos - startPos
	if nByte <= bytesPerVec {
		for pos := startPos; pos < endPos; pos++ {
			if arg[pos] <= val {
				return pos
			}
		}
		return endPos
	}
	argHeader := (*reflect.SliceHeader)(unsafe.Pointer(&arg))
	return firstLeq8SSSE3Asm(unsafe.Pointer(argHeader.Data), int(val), startPos, endPos)
}
