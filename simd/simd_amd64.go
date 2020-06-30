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
	"golang.org/x/sys/cpu"
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

// This must be at least <maximum supported vector size> / 16.
const nibbleLookupDup = 1

// NibbleLookupTable represents a parallel-byte-substitution operation f, where
// every byte b in a byte-slice is replaced with
//   f(b) := shuffle[0][b & 15] for b <= 127, and
//   f(b) := 0 for b > 127.
// (The second part is usually irrelevant in practice, but must be defined this
// way to allow _mm_shuffle_epi8()/_mm256_shuffle_epi8()/_mm512_shuffle_epi8()
// to be used to implement the operation efficiently.)
// It's named NibbleLookupTable rather than ByteLookupTable since only the
// bottom nibble of each byte can be used for table lookup.
// It potentially stores multiple adjacent copies of the lookup table since
// that speeds up the AVX2 and AVX-512 use cases (the table can be loaded with
// a single _mm256_loadu_si256 operation, instead of e.g. _mm_loadu_si128
// followed by _mm256_set_m128i with the same argument twice), and the typical
// use case involves initializing very few tables and using them many, many
// times.
type NibbleLookupTable struct {
	shuffle [nibbleLookupDup][16]byte
}

// Get performs the b <= 127 part of the lookup operation described above.
// The b > 127 branch is omitted because in many use cases (e.g.
// PackedNibbleLookup below), it can be proven that b > 127 is impossible, and
// removing the if-statement is a significant performance win when it's
// possible.
func (t *NibbleLookupTable) Get(b byte) byte {
	return t.shuffle[0][b]
}

// const minPageSize = 4096  may be relevant for safe functions soon.

// bytesPerVec is the size of the maximum-width vector that may be used.  It is
// at least 16, but will soon be set to 32 if AVX2 support is detected.  It
// may be set to 64 in the future when AVX-512 is detected.
var bytesPerVec int

// log2BytesPerVec supports efficient division by bytesPerVec.
var log2BytesPerVec uint

// *** the following functions are defined in simd_amd64.s

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

//go:noescape
func bitFromEveryByteSSE2Asm(dst, src unsafe.Pointer, lshift, nDstByte int)

// *** end assembly function signatures

func init() {
	if !cpu.X86.HasSSE42 {
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
// potentially-size-increasing operation on dst[] is {Make,Remake}Unsafe(),
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

// MakeNibbleLookupTable generates a NibbleLookupTable from a [16]byte.
func MakeNibbleLookupTable(table [16]byte) (t NibbleLookupTable) {
	for i := range t.shuffle {
		t.shuffle[i] = table
	}
	return
}

// UnpackedNibbleLookupUnsafeInplace replaces the bytes in main[] as follows:
//   if value < 128, set to table[value & 15]
//   otherwise, set to 0
//
// WARNING: This is a function designed to be used in inner loops, which makes
// assumptions about capacity which aren't checked at runtime.  Use the safe
// version of this function when that's a problem.
// These assumptions are always satisfied when the last
// potentially-size-increasing operation on main[] is {Make,Remake}Unsafe(),
// ResizeUnsafe(), or XcapUnsafe().
//
// 1. cap(main) must be at least RoundUpPow2(len(main) + 1, bytesPerVec).
//
// 2. The caller does not care if a few bytes past the end of main[] are
// changed.
func UnpackedNibbleLookupUnsafeInplace(main []byte, tablePtr *NibbleLookupTable) {
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
func UnpackedNibbleLookupInplace(main []byte, tablePtr *NibbleLookupTable) {
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
				curByte = tablePtr.Get(curByte & 15)
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
func UnpackedNibbleLookupUnsafe(dst, src []byte, tablePtr *NibbleLookupTable) {
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	unpackedNibbleLookupSSSE3Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), unsafe.Pointer(tablePtr), srcHeader.Len)
}

// UnpackedNibbleLookup sets the bytes in dst[] as follows:
//   if src[pos] < 128, set dst[pos] := table[src[pos] & 15]
//   otherwise, set dst[pos] := 0
// It panics if len(src) != len(dst).
func UnpackedNibbleLookup(dst, src []byte, tablePtr *NibbleLookupTable) {
	srcLen := len(src)
	if len(dst) != srcLen {
		panic("UnpackedNibbleLookup() requires len(src) == len(dst).")
	}
	if srcLen < 16 {
		for pos, curByte := range src {
			if curByte < 128 {
				curByte = tablePtr.Get(curByte & 15)
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

// UnpackedNibbleLookupS is a variant of UnpackedNibbleLookup() that takes
// string src.
func UnpackedNibbleLookupS(dst []byte, src string, tablePtr *NibbleLookupTable) {
	srcLen := len(src)
	if len(dst) != srcLen {
		panic("UnpackedNibbleLookupS() requires len(src) == len(dst).")
	}
	if srcLen < 16 {
		for pos := range src {
			curByte := src[pos]
			if curByte < 128 {
				curByte = tablePtr.Get(curByte & 15)
			} else {
				curByte = 0
			}
			dst[pos] = curByte
		}
		return
	}
	srcHeader := (*reflect.StringHeader)(unsafe.Pointer(&src))
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
func PackedNibbleLookupUnsafe(dst, src []byte, tablePtr *NibbleLookupTable) {
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
func PackedNibbleLookup(dst, src []byte, tablePtr *NibbleLookupTable) {
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
			dst[2*srcPos] = tablePtr.Get(srcByte & 15)
			dst[2*srcPos+1] = tablePtr.Get(srcByte >> 4)
		}
	} else {
		srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
		dstHeader := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
		packedNibbleLookupOddSSSE3Asm(unsafe.Pointer(dstHeader.Data), unsafe.Pointer(srcHeader.Data), unsafe.Pointer(tablePtr), nSrcFullByte)
	}
	if srcOdd == 1 {
		srcByte := src[nSrcFullByte]
		dst[2*nSrcFullByte] = tablePtr.Get(srcByte & 15)
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

// BitFromEveryByte fills dst[] with a bitarray containing every 8th bit from
// src[], starting with bitIdx, where bitIdx is in [0,7].  If len(src) is not
// divisible by 8, extra bits in the last filled byte of dst are set to zero.
//
// For example, if src[] is
//   0x1f 0x33 0x0d 0x00 0x51 0xcc 0x34 0x59 0x44
// and bitIdx is 2, bit 2 from every byte is
//      1    0    1    0    0    1    1    0    1
// so dst[] is filled with
//   0x65 0x01.
//
// - It panics if len(dst) < (len(src) + 7) / 8, or bitIdx isn't in [0,7].
// - If dst is larger than necessary, the extra bytes are not changed.
func BitFromEveryByte(dst, src []byte, bitIdx int) {
	requiredDstLen := (len(src) + 7) >> 3
	if (len(dst) < requiredDstLen) || (uint(bitIdx) > 7) {
		panic("BitFromEveryByte requires len(dst) >= (len(src) + 7) / 8 and 0 <= bitIdx < 8.")
	}
	nSrcVecByte := len(src) &^ (bytesPerVec - 1)
	if nSrcVecByte != 0 {
		bitFromEveryByteSSE2Asm(unsafe.Pointer(&dst[0]), unsafe.Pointer(&src[0]), 7-bitIdx, nSrcVecByte>>3)
	}
	remainder := len(src) - nSrcVecByte
	if remainder != 0 {
		// Not optimized since it isn't expected to matter.
		srcLast := src[nSrcVecByte:]
		dstLast := dst[nSrcVecByte>>3 : requiredDstLen]
		for i := range dstLast {
			dstLast[i] = 0
		}
		for i, b := range srcLast {
			dstLast[i>>3] |= ((b >> uint32(bitIdx)) & 1) << uint32(i&7)
		}
	}
}
