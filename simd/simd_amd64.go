// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build amd64,!appengine

package simd

import (
	"math/bits"
	"reflect"
	"unsafe"

	gunsafe "github.com/grailbio/base/unsafe"
)

// amd64 compile-time constants.

// BytesPerWord is the number of bytes in a machine word.
// We don't use unsafe.Sizeof(uintptr(1)) since there are advantages to having
// this as an untyped constant, and there's essentially no drawback since this
// is an _amd64-specific file.
const BytesPerWord = 8

// Log2BytesPerWord is log2(BytesPerWord).  This is relevant for manual
// bit-shifting when we know that's a safe way to divide and the compiler does
// not (e.g. dividend is of signed int type).
const Log2BytesPerWord = uint(3)

// BitsPerWord is the number of bits in a machine word.
const BitsPerWord = BytesPerWord * 8

// const minPageSize = 4096  may be relevant for safe functions soon.

// These could be compile-time constants for now, but not after AVX2
// autodetection is added.

// bytesPerVec is the size of the maximum-width vector that may be used.  It is
// currently always 16, but it will be set to larger values at runtime in the
// future when AVX2/AVX-512/etc. is detected.
var bytesPerVec int

// log2BytesPerVec supports efficient division by bytesPerVec.
var log2BytesPerVec uint

// *** the following functions are defined in simd_amd64.s

// Strictly speaking, hasSSE42Asm() duplicates code in e.g.
// github.com/klauspost/cpuid , but it's literally only a few bytes.
// Todo: look into replacing this with go:linkname exploitation of the
// runtime's cpuid check results, and empty import of runtime.

//go:noescape
func hasSSE42Asm() bool

// There was a unpackedNibbleLookupInplaceSSSE3Asm function here, but it
// actually benchmarked worse than the general-case function.

//go:noescape
func unpackedNibbleLookupTinyInplaceSSSE3Asm(main, tablePtr unsafe.Pointer)

//go:noescape
func unpackedNibbleLookupOddInplaceSSSE3Asm(main, tablePtr unsafe.Pointer, nByte int)

//go:noescape
func unpackedNibbleLookupSSSE3Asm(dst, src, tablePtr unsafe.Pointer, nByte int)

//go:noescape
func unpackedNibbleLookupOddSSSE3Asm(dst, src, tablePtr unsafe.Pointer, nByte int)

//go:noescape
func packedNibbleLookupSSSE3Asm(dst, src, tablePtr unsafe.Pointer, nSrcByte int)

//go:noescape
func packedNibbleLookupOddSSSE3Asm(dst, src, tablePtr unsafe.Pointer, nSrcFullByte int)

//go:noescape
func interleave8SSE2Asm(dst, even, odd unsafe.Pointer, nDstByte int)

//go:noescape
func interleave8OddSSE2Asm(dst, even, odd unsafe.Pointer, nOddByte int)

//go:noescape
func reverse8InplaceSSSE3Asm(main unsafe.Pointer, nByte int)

//go:noescape
func reverse8SSSE3Asm(dst, src unsafe.Pointer, nByte int)

// *** end assembly function signatures

func init() {
	if !hasSSE42Asm() {
		panic("SSE4.2 required.")
	}
	bytesPerVec = 16
	log2BytesPerVec = 4
}

// BytesPerVec is an accessor for the bytesPerVec package variable.
func BytesPerVec() int {
	return bytesPerVec
}

// RoundUpPow2 returns val rounded up to a multiple of alignment, assuming
// alignment is a power of 2.
func RoundUpPow2(val, alignment int) int {
	return (val + alignment - 1) & (^(alignment - 1))
}

// DivUpPow2 efficiently divides a number by a power-of-2 divisor.  (This works
// for negative dividends since the language specifies arithmetic right-shifts
// of signed numbers.  I'm pretty sure this doesn't have a performance
// penalty.)
func DivUpPow2(dividend, divisor int, log2Divisor uint) int {
	return (dividend + divisor - 1) >> log2Divisor
}

// MakeUnsafe returns a byte slice of the given length which is guaranteed to
// have enough capacity for all Unsafe functions in this package to work.  (It
// is not itself an unsafe function: allocated memory is zero-initialized.)
// Note that Unsafe functions occasionally have other caveats: e.g.
// PopcntUnsafe also requires relevant bytes past the end of the slice to be
// zeroed out.
func MakeUnsafe(len int) []byte {
	// Although no planned function requires more than
	// RoundUpPow2(len+1, bytesPerVec) capacity, it is necessary to add
	// bytesPerVec instead to make subslicing safe.
	return make([]byte, len, len+bytesPerVec)
}

// RemakeUnsafe reuses the given buffer if it has sufficient capacity;
// otherwise it does the same thing as MakeUnsafe.  It does NOT preserve
// existing contents of buf[]; use ResizeUnsafe() for that.
func RemakeUnsafe(bufptr *[]byte, len int) {
	minCap := len + bytesPerVec
	if minCap <= cap(*bufptr) {
		gunsafe.ExtendBytes(bufptr, len)
		return
	}
	// This is likely to be called in an inner loop processing variable-size
	// inputs, so mild exponential growth is appropriate.
	*bufptr = make([]byte, len, RoundUpPow2(minCap+(minCap/8), bytesPerVec))
}

// ResizeUnsafe changes the length of buf and ensures it has enough extra
// capacity to be passed to this package's Unsafe functions.  Existing buf[]
// contents are preserved (with possible truncation), though when length is
// increased, new bytes might not be zero-initialized.
func ResizeUnsafe(bufptr *[]byte, len int) {
	minCap := len + bytesPerVec
	if minCap <= cap(*bufptr) {
		gunsafe.ExtendBytes(bufptr, len)
		return
	}
	dst := make([]byte, len, RoundUpPow2(minCap+(minCap/8), bytesPerVec))
	copy(dst, *bufptr)
	*bufptr = dst
}

// XcapUnsafe is shorthand for ResizeUnsafe's most common use case (no length
// change, just want to ensure sufficient capacity).
func XcapUnsafe(bufptr *[]byte) {
	// mid-stack inlining isn't yet working as I write this, but it should be
	// available soon enough:
	//   https://github.com/golang/go/issues/19348
	ResizeUnsafe(bufptr, len(*bufptr))
}

// Memset8Unsafe sets all values of dst[] to the given byte.  (This is intended
// for val != 0.  It is better to use a range-for loop for val == 0 since the
// compiler has a hardcoded optimization for that case; see
// https://github.com/golang/go/issues/5373 .)
//
// WARNING: This is a function designed to be used in inner loops, which
// assumes without checking that capacity is at least RoundUpPow2(len(dst),
// bytesPerVec).  It also assumes that the caller does not care if a few bytes
// past the end of dst[] are changed.  Use the safe version of this function if
// any of these properties are problematic.
// These assumptions are always satisfied when the last
// potentially-size-increasing operation on dst[] is {Re}makeUnsafe(),
// ResizeUnsafe(), or XcapUnsafe().
func Memset8Unsafe(dst []byte, val byte) {
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	valWord := uintptr(0x0101010101010101) * uintptr(val)
	// Compiler optimizes this well, my first attempt at a SSE implementation did
	// not do better on my Mac, and neither did a non-AVX2 direct copy of
	// runtime.memclr_amd64.
	// With that said, benchmark against memclr reveals that AVX2 (and
	// non-temporal stores in the >32 MiB case) makes a significant difference.
	nWord := DivUpPow2(len(dst), BytesPerWord, Log2BytesPerWord)
	dstWordsIter := unsafe.Pointer(dstHeader.Data)
	for widx := 0; widx < nWord; widx++ {
		*((*uintptr)(dstWordsIter)) = valWord
		dstWordsIter = unsafe.Pointer(uintptr(dstWordsIter) + BytesPerWord)
	}
}

// Memset8 sets all values of dst[] to the given byte.  (This is intended for
// val != 0.  It is better to use a range-for loop for val == 0 since the
// compiler has a hardcoded optimization for that case.)
func Memset8(dst []byte, val byte) {
	// This is ~2-8% slower than the unsafe version.
	dstLen := len(dst)
	if dstLen < BytesPerWord {
		for pos := range dst {
			dst[pos] = val
		}
		return
	}
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	valWord := uintptr(0x0101010101010101) * uintptr(val)
	nWordMinus1 := (dstLen - 1) >> Log2BytesPerWord
	dstWordsIter := unsafe.Pointer(dstHeader.Data)
	for widx := 0; widx < nWordMinus1; widx++ {
		*((*uintptr)(dstWordsIter)) = valWord
		dstWordsIter = unsafe.Pointer(uintptr(dstWordsIter) + BytesPerWord)
	}
	dstWordsIter = unsafe.Pointer(dstHeader.Data + uintptr(dstLen) - BytesPerWord)
	*((*uintptr)(dstWordsIter)) = valWord
}

// UnpackedNibbleLookupUnsafeInplace replaces the bytes in main[] as follows:
//   if value < 128, set to table[value & 15]
//   otherwise, set to 0
//
// WARNING: This is a function designed to be used in inner loops, which makes
// assumptions about capacity which aren't checked at runtime.  Use the safe
// version of this function when that's a problem.
// These assumptions are always satisfied when the last
// potentially-size-increasing operation on main[] is {Re}makeUnsafe(),
// ResizeUnsafe(), or XcapUnsafe().
//
// 1. cap(main) must be at least RoundUpPow2(len(main) + 1, bytesPerVec).
//
// 2. The caller does not care if a few bytes past the end of main[] are
// changed.
func UnpackedNibbleLookupUnsafeInplace(main []byte, tablePtr *[16]byte) {
	mainLen := len(main)
	mainHeader := (*reflect.SliceHeader)(unsafe.Pointer(&main))
	if mainLen <= 16 {
		// originally just set mainLen = bytesPerVec and rejoined the main branch,
		// but that produced noticeably worse benchmark results, even for the usual
		// case.
		unpackedNibbleLookupTinyInplaceSSSE3Asm(unsafe.Pointer(mainHeader.Data), unsafe.Pointer(tablePtr))
		return
	}
	unpackedNibbleLookupOddInplaceSSSE3Asm(unsafe.Pointer(mainHeader.Data), unsafe.Pointer(tablePtr), mainLen)
}

// UnpackedNibbleLookupInplace replaces the bytes in main[] as follows:
//   if value < 128, set to table[value & 15]
//   otherwise, set to 0
func UnpackedNibbleLookupInplace(main []byte, tablePtr *[16]byte) {
	// May want to define variants of these functions which have undefined
	// results for input values in [16, 128); this will be useful for
	// cross-platform ARM/x86 code.
	mainLen := len(main)
	if mainLen < 16 {
		// Tried copying to and from a [16]byte, overhead of that was too high.
		// (I consider the poor performance of this case to be one of the strongest
		// justifications for exporting Unsafe functions at all.)
		for pos, curByte := range main {
			if curByte < 128 {
				curByte = tablePtr[curByte&15]
			} else {
				curByte = 0
			}
			main[pos] = curByte
		}
		return
	}
	mainHeader := (*reflect.SliceHeader)(unsafe.Pointer(&main))
	unpackedNibbleLookupOddInplaceSSSE3Asm(unsafe.Pointer(mainHeader.Data), unsafe.Pointer(tablePtr), mainLen)
}

// UnpackedNibbleLookupUnsafe sets the bytes in dst[] as follows:
//   if src[pos] < 128, set dst[pos] := table[src[pos] & 15]
//   otherwise, set dst[pos] := 0
//
// WARNING: This is a function designed to be used in inner loops, which makes
// assumptions about length and capacity which aren't checked at runtime.  Use
// the safe version of this function when that's a problem.
// Assumptions #2-3 are always satisfied when the last
// potentially-size-increasing operation on src[] is {Re}makeUnsafe(),
// ResizeUnsafe(), or XcapUnsafe(), and the same is true for dst[].
//
// 1. len(src) and len(dst) are equal.
//
// 2. Capacities are at least RoundUpPow2(len(src) + 1, bytesPerVec).
//
// 3. The caller does not care if a few bytes past the end of dst[] are
// changed.
func UnpackedNibbleLookupUnsafe(dst, src []byte, tablePtr *[16]byte) {
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	unpackedNibbleLookupSSSE3Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), unsafe.Pointer(tablePtr), srcHeader.Len)
}

// UnpackedNibbleLookup sets the bytes in dst[] as follows:
//   if src[pos] < 128, set dst[pos] := table[src[pos] & 15]
//   otherwise, set dst[pos] := 0
// It panics if len(src) != len(dst).
func UnpackedNibbleLookup(dst, src []byte, tablePtr *[16]byte) {
	srcLen := len(src)
	if len(dst) != srcLen {
		panic("UnpackedNibbleLookup() requires len(src) == len(dst).")
	}
	if srcLen < 16 {
		for pos, curByte := range src {
			if curByte < 128 {
				curByte = tablePtr[curByte&15]
			} else {
				curByte = 0
			}
			dst[pos] = curByte
		}
		return
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	unpackedNibbleLookupOddSSSE3Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), unsafe.Pointer(tablePtr), srcLen)
}

// PackedNibbleLookupUnsafe sets the bytes in dst[] as follows:
//   if pos is even, dst[pos] := table[src[pos / 2] & 15]
//   if pos is odd, dst[pos] := table[src[pos / 2] >> 4]
//
// WARNING: This is a function designed to be used in inner loops, which makes
// assumptions about length and capacity which aren't checked at runtime.  Use
// the safe version of this function when that's a problem.
// Assumptions #2-#3 are always satisfied when the last
// potentially-size-increasing operation on src[] is {Re}makeUnsafe(),
// ResizeUnsafe(), or XcapUnsafe(), and the same is true for dst[].
//
// 1. len(src) == (len(dst) + 1) / 2.
//
// 2. Capacity of src is at least RoundUpPow2(len(src) + 1, bytesPerVec), and
// the same is true for dst.
//
// 3. The caller does not care if a few bytes past the end of dst[] are
// changed.
func PackedNibbleLookupUnsafe(dst, src []byte, tablePtr *[16]byte) {
	// Note that this is not the correct order for .bam seq[] unpacking; use
	// biosimd.UnpackAndReplaceSeqUnsafe() for that.
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	packedNibbleLookupSSSE3Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), unsafe.Pointer(tablePtr), srcHeader.Len)
}

// PackedNibbleLookup sets the bytes in dst[] as follows:
//   if pos is even, dst[pos] := table[src[pos / 2] & 15]
//   if pos is odd, dst[pos] := table[src[pos / 2] >> 4]
// It panics if len(src) != (len(dst) + 1) / 2.
//
// Nothing bad happens if len(dst) is odd and some high bits in the last src[]
// byte are set, though it's generally good practice to ensure that case
// doesn't come up.
func PackedNibbleLookup(dst, src []byte, tablePtr *[16]byte) {
	// This takes ~15% longer than the unsafe function on the short-array
	// benchmark.
	dstLen := len(dst)
	nSrcFullByte := dstLen >> 1
	srcOdd := dstLen & 1
	if len(src) != nSrcFullByte+srcOdd {
		panic("PackedNibbleLookup() requires len(src) == (len(dst) + 1) / 2.")
	}
	if nSrcFullByte < 16 {
		for srcPos := 0; srcPos < nSrcFullByte; srcPos++ {
			srcByte := src[srcPos]
			dst[2*srcPos] = tablePtr[srcByte&15]
			dst[2*srcPos+1] = tablePtr[srcByte>>4]
		}
	} else {
		srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
		dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
		packedNibbleLookupOddSSSE3Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), unsafe.Pointer(tablePtr), nSrcFullByte)
	}
	if srcOdd == 1 {
		srcByte := src[nSrcFullByte]
		dst[2*nSrcFullByte] = tablePtr[srcByte&15]
	}
}

// Interleave8Unsafe sets the bytes in dst[] as follows:
//   if pos is even, dst[pos] := even[pos/2]
//   if pos is odd, dst[pos] := odd[pos/2]
//
// WARNING: This is a function designed to be used in inner loops, which makes
// assumptions about length and capacity which aren't checked at runtime.  Use
// the safe version of this function when that's a problem.
// Assumptions #2-3 are always satisfied when the last
// potentially-size-increasing operation on dst[] is {Re}makeUnsafe(),
// ResizeUnsafe(), or XcapUnsafe(), and the same is true for even[] and odd[].
//
// 1. len(even) = (len(dst) + 1) / 2, and len(odd) = len(dst) / 2.
//
// 2. cap(dst) >= RoundUpPow2(len(dst) + 1, bytesPerVec),
// cap(even) >= RoundUpPow2(len(even) + 1, bytesPerVec), and
// cap(odd) >= RoundUpPow2(len(odd) + 1, bytesPerVec).
//
// 3. The caller does not care if a few bytes past the end of dst[] are
// changed.
func Interleave8Unsafe(dst, even, odd []byte) {
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	evenHeader := (*reflect.SliceHeader)(unsafe.Pointer(&even))
	oddHeader := (*reflect.SliceHeader)(unsafe.Pointer(&odd))
	interleave8SSE2Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(evenHeader.Data), unsafe.Pointer(oddHeader.Data), dstHeader.Len)
}

// Interleave8 sets the bytes in dst[] as follows:
//   if pos is even, dst[pos] := even[pos/2]
//   if pos is odd, dst[pos] := odd[pos/2]
// It panics if ((len(dst) + 1) / 2) != len(even), or (len(dst) / 2) !=
// len(odd).
func Interleave8(dst, even, odd []byte) {
	// This is ~6-20% slower than the unsafe function on the short-array
	// benchmark.
	dstLen := len(dst)
	evenLen := (dstLen + 1) >> 1
	oddLen := dstLen >> 1
	if (len(even) != evenLen) || (len(odd) != oddLen) {
		panic("Interleave8() requires len(even) == len(dst) + 1) / 2, and len(odd) == len(dst) / 2.")
	}
	if oddLen < 16 {
		for idx, oddByte := range odd {
			dst[2*idx] = even[idx]
			dst[2*idx+1] = oddByte
		}
	} else {
		dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
		evenHeader := (*reflect.SliceHeader)(unsafe.Pointer(&even))
		oddHeader := (*reflect.SliceHeader)(unsafe.Pointer(&odd))
		interleave8OddSSE2Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(evenHeader.Data), unsafe.Pointer(oddHeader.Data), oddLen)
	}
	if oddLen != evenLen {
		dst[oddLen*2] = even[oddLen]
	}
}

// Reverse8Inplace reverses the bytes in main[].  (There is no unsafe version
// of this function.)
func Reverse8Inplace(main []byte) {
	mainHeader := (*reflect.SliceHeader)(unsafe.Pointer(&main))
	reverse8InplaceSSSE3Asm(unsafe.Pointer(mainHeader.Data), mainHeader.Len)
}

// Reverse8Unsafe sets dst[pos] := src[len(src) - 1 - pos] for every position
// in src.
//
// WARNING: This does not verify len(dst) == len(src); call the safe version of
// this function if you want that.
func Reverse8Unsafe(dst, src []byte) {
	nByte := len(src)
	if nByte < BytesPerWord {
		// could use bswap32 on two uint32s if nByte in 4..7
		nByteMinus1 := nByte - 1
		for idx := 0; idx != nByte; idx++ {
			dst[nByteMinus1-idx] = src[idx]
		}
		return
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	if nByte < 16 {
		// use bswap64 on a word at a time
		nWordMinus1 := (nByte - 1) >> Log2BytesPerWord
		finalOffset := uintptr(nByte) - BytesPerWord
		srcIter := unsafe.Pointer(srcHeader.Data + finalOffset)
		dstIter := unsafe.Pointer(dstHeader.Data)
		for widx := 0; widx < nWordMinus1; widx++ {
			srcWord := *((*uintptr)(srcIter))
			*((*uintptr)(dstIter)) = uintptr(bits.ReverseBytes64(uint64(srcWord)))
			srcIter = unsafe.Pointer(uintptr(srcIter) - BytesPerWord)
			dstIter = unsafe.Pointer(uintptr(dstIter) - BytesPerWord)
		}
		srcFirstWordPtr := unsafe.Pointer(srcHeader.Data)
		dstLastWordPtr := unsafe.Pointer(dstHeader.Data + finalOffset)
		srcWord := *((*uintptr)(srcFirstWordPtr))
		*((*uintptr)(dstLastWordPtr)) = uintptr(bits.ReverseBytes64(uint64(srcWord)))
		return
	}
	reverse8SSSE3Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), nByte)
}

// Reverse8 sets dst[pos] := src[len(src) - 1 - pos] for every position in src.
// It panics if len(src) != len(dst).
func Reverse8(dst, src []byte) {
	nByte := len(src)
	if nByte != len(dst) {
		panic("Reverse8() requires len(src) == len(dst).")
	}
	if nByte < BytesPerWord {
		// could use bswap32 on two uint32s if nByte in 4..7
		nByteMinus1 := nByte - 1
		for idx := 0; idx != nByte; idx++ {
			dst[nByteMinus1-idx] = src[idx]
		}
		return
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	if nByte < 16 {
		// use bswap64 on a word at a time
		nWordMinus1 := (nByte - 1) >> Log2BytesPerWord
		finalOffset := uintptr(nByte) - BytesPerWord
		srcIter := unsafe.Pointer(srcHeader.Data + finalOffset)
		dstIter := unsafe.Pointer(dstHeader.Data)
		for widx := 0; widx < nWordMinus1; widx++ {
			srcWord := *((*uintptr)(srcIter))
			*((*uintptr)(dstIter)) = uintptr(bits.ReverseBytes64(uint64(srcWord)))
			srcIter = unsafe.Pointer(uintptr(srcIter) - BytesPerWord)
			dstIter = unsafe.Pointer(uintptr(dstIter) - BytesPerWord)
		}
		srcFirstWordPtr := unsafe.Pointer(srcHeader.Data)
		dstLastWordPtr := unsafe.Pointer(dstHeader.Data + finalOffset)
		srcWord := *((*uintptr)(srcFirstWordPtr))
		*((*uintptr)(dstLastWordPtr)) = uintptr(bits.ReverseBytes64(uint64(srcWord)))
		return
	}
	reverse8SSSE3Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), nByte)
}
