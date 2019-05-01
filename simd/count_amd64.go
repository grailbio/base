// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build amd64,!appengine

// This is derived from github.com/willf/bitset .

package simd

import "math/bits"
import "reflect"
import "unsafe"

// *** the following function is defined in count_amd64.s

//go:noescape
func popcntWordArraySSE42Asm(bytes unsafe.Pointer, nWord int) int

// Although mask and val are really byte parameters, actually declaring them as
// bytes instead of ints in the function signature produces a *massive*
// performance penalty.

//go:noescape
func maskThenCountByteSSE41Asm(src unsafe.Pointer, mask, val, nByte int) int

//go:noescape
func count2BytesSSE41Asm(src unsafe.Pointer, val1, val2, nByte int) int

//go:noescape
func count3BytesSSE41Asm(src unsafe.Pointer, val1, val2, val3, nByte int) int

//go:noescape
func countNibblesInSetSSE41Asm(src unsafe.Pointer, tablePtr *NibbleLookupTable, nByte int) int

func countNibblesInTwoSetsSSE41Asm(cnt2Ptr *int, src unsafe.Pointer, table1Ptr, table2Ptr *NibbleLookupTable, nByte int) int

//go:noescape
func countUnpackedNibblesInSetSSE41Asm(src unsafe.Pointer, tablePtr *NibbleLookupTable, nByte int) int

//go:noescape
func countUnpackedNibblesInTwoSetsSSE41Asm(cnt2Ptr *int, src unsafe.Pointer, table1Ptr, table2Ptr *NibbleLookupTable, nByte int) int

//go:noescape
func accumulate8SSE41Asm(src unsafe.Pointer, nByte int) int

//go:noescape
func accumulate8GreaterSSE41Asm(src unsafe.Pointer, val, nByte int) int

// *** end assembly function signature(s)

// PopcntUnsafe returns the number of set bits in the given []byte, assuming
// that any trailing bytes up to the next multiple of BytesPerWord are zeroed
// out.
func PopcntUnsafe(bytes []byte) int {
	// Get the base-pointer for the slice, in a way that doesn't trigger a
	// bounds-check and fail when length == 0.  (Yes, I found out during testing
	// that the &bytes[0] idiom doesn't actually work in the length-0
	// case...)
	bytesHeader := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))

	return popcntWordArraySSE42Asm(unsafe.Pointer(bytesHeader.Data), DivUpPow2(len(bytes), BytesPerWord, Log2BytesPerWord))
}

// Popcnt returns the number of set bits in the given []byte.
//
// Some effort has been made to make this run acceptably fast on relatively
// short arrays, since I expect knowing how to do so to be helpful when working
// with hundreds of millions of .bam reads with ~75 bytes of base data and ~150
// bytes of quality data.  Interestingly, moving the leading-byte handling code
// to assembly didn't make a difference.
//
// Some single-threaded benchmark results calling Popcnt 99999999 times on a
// 14-byte unaligned array:
//   C implementation: 0.219-0.232s
//   This code: 0.606-0.620s
//   C implementation using memcpy for trailing bytes: 0.964-0.983s
// So Go's extra looping and function call overhead can almost triple runtime
// in the short-array limit, but that's actually not as bad as the 4.5x
// overhead of trusting memcpy to handle trailing bytes.
func Popcnt(bytes []byte) int {
	bytesHeader := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	nByte := len(bytes)

	bytearr := unsafe.Pointer(bytesHeader.Data)
	tot := 0
	nLeadingByte := nByte & (BytesPerWord - 1)
	if nLeadingByte != 0 {
		leadingWord := uint64(0)
		if (nLeadingByte & 1) != 0 {
			leadingWord = (uint64)(*(*byte)(bytearr))
			bytearr = unsafe.Pointer(uintptr(bytearr) + 1)
		}
		if (nLeadingByte & 2) != 0 {
			// Note that this does not keep the bytes in the original little-endian
			// order, since that's irrelevant for popcount, and probably everything
			// else we need to do in this package.  See ProperSubwordLoad() in
			// plink2_base.h for code which does keep the bytes in order.
			leadingWord <<= 16
			leadingWord |= (uint64)(*(*uint16)(bytearr))
			bytearr = unsafe.Pointer(uintptr(bytearr) + 2)
		}
		if (nLeadingByte & 4) != 0 {
			leadingWord <<= 32
			leadingWord |= (uint64)(*(*uint32)(bytearr))
			bytearr = unsafe.Pointer(uintptr(bytearr) + 4)
		}
		tot = bits.OnesCount64(leadingWord)
	}
	tot += popcntWordArraySSE42Asm(bytearr, nByte>>Log2BytesPerWord)
	return tot
}

// We may want a PopcntW function in the future which operates on a []uintptr,
// along with AndW, OrW, XorW, InvmaskW, etc.  This would amount to a
// lower-overhead version of willf/bitset (which also uses []uintptr
// internally).
// The main thing I would want to benchmark before making that decision is
// bitset.NextSetMany() vs. a loop of the form
//   uidx_base := 0
//   cur_bits := bitarr[0]
//   for idx := 0; idx != nSetBit; idx++ {
//     // see plink2_base.h BitIter1()
//     if cur_bits == 0 {
//       widx := uidx_base >> (3 + Log2BytesPerWord)
//       for {
//         widx++
//         cur_bits = bitarr[widx]
//         if cur_bits != 0 {
//           break
//         }
//       }
//       uidx_base = widx << (3 + Log2BytesPerWord)
//     }
//     uidx := uidx_base + bits.TrailingZeros(uint(cur_bits))
//     cur_bits = cur_bits & (cur_bits - 1)
//     // (do something with uidx, possibly very simple)
//   }
// (Note that there are *hundreds* of loops of this form in plink2.)
// If bitset.NextSetMany() does not impose a large performance penalty, we may
// just want to write a version of it which takes a []byte as input.
// (update: https://go-review.googlesource.com/c/go/+/109716 suggests that
// bitset.NextSetMany() is not good enough.)

// todo: add ZeroTrailingBits, etc. once we need it

// MaskThenCountByte counts the number of bytes in src[] satisfying
//   src[pos] & mask == val.
func MaskThenCountByte(src []byte, mask, val byte) int {
	// This is especially useful for CG counting:
	// - Count 'C'/'G' ASCII characters: mask = 0xfb (only capital) or 0xdb
	//   (either capital or lowercase), val = 'C'
	// - Count C/G bytes in .bam unpacked seq8 data, assuming '=' is not in
	//   input: mask = 0x9, val = 0
	// It can also be used to ignore capitalization when counting instances of a
	// single letter.
	nByte := len(src)
	if nByte < 16 {
		cnt := 0
		for _, srcByte := range src {
			if (srcByte & mask) == val {
				cnt++
			}
		}
		return cnt
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	return maskThenCountByteSSE41Asm(unsafe.Pointer(srcHeader.Data), int(mask), int(val), nByte)
}

// Count2Bytes counts the number of bytes in src[] which are equal to either
// val1 or val2.
// (bytes.Count() should be good enough for a single byte.)
func Count2Bytes(src []byte, val1, val2 byte) int {
	nByte := len(src)
	if nByte < 16 {
		cnt := 0
		for _, srcByte := range src {
			if (srcByte == val1) || (srcByte == val2) {
				cnt++
			}
		}
		return cnt
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	return count2BytesSSE41Asm(unsafe.Pointer(srcHeader.Data), int(val1), int(val2), nByte)
}

// Count3Bytes counts the number of bytes in src[] which are equal to val1,
// val2, or val3.
func Count3Bytes(src []byte, val1, val2, val3 byte) int {
	nByte := len(src)
	if nByte < 16 {
		cnt := 0
		for _, srcByte := range src {
			if (srcByte == val1) || (srcByte == val2) || (srcByte == val3) {
				cnt++
			}
		}
		return cnt
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	return count3BytesSSE41Asm(unsafe.Pointer(srcHeader.Data), int(val1), int(val2), int(val3), nByte)
}

// CountNibblesInSet counts the number of nibbles in src[] which are in the
// given set.  The set must be represented as table[x] == 1 when value x is in
// the set, and table[x] == 0 when x isn't.
//
// WARNING: This function does not validate the table.  It may return a garbage
// result on invalid input.  (However, it won't corrupt memory.)
func CountNibblesInSet(src []byte, tablePtr *NibbleLookupTable) int {
	nSrcByte := len(src)
	if nSrcByte < 16 {
		cnt := 0
		for _, srcByte := range src {
			cnt += int(tablePtr.Get(srcByte&15) + tablePtr.Get(srcByte>>4))
		}
		return cnt
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	return countNibblesInSetSSE41Asm(unsafe.Pointer(srcHeader.Data), tablePtr, nSrcByte)
}

// CountNibblesInTwoSets counts the number of bytes in src[] which are in the
// given two sets, assuming all bytes are <16.  The sets must be represented as
// table[x] == 1 when value x is in the set, and table[x] == 0 when x isn't.
//
// WARNING: This function does not validate the tables.  It may crash or return
// garbage results on invalid input.  (However, it won't corrupt memory.)
func CountNibblesInTwoSets(src []byte, table1Ptr, table2Ptr *NibbleLookupTable) (int, int) {
	nSrcByte := len(src)
	cnt2 := 0
	if nSrcByte < 16 {
		cnt1 := 0
		for _, srcByte := range src {
			lowBits := srcByte & 15
			highBits := srcByte >> 4
			cnt1 += int(table1Ptr.Get(lowBits) + table1Ptr.Get(highBits))
			cnt2 += int(table2Ptr.Get(lowBits) + table2Ptr.Get(highBits))
		}
		return cnt1, cnt2
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	cnt1 := countNibblesInTwoSetsSSE41Asm(&cnt2, unsafe.Pointer(srcHeader.Data), table1Ptr, table2Ptr, nSrcByte)
	return cnt1, cnt2
}

// CountUnpackedNibblesInSet counts the number of bytes in src[] which are in
// the given set, assuming all bytes are <16.  The set must be represented as
// table[x] == 1 when value x is in the set, and table[x] == 0 when x isn't.
//
// WARNING: This function does not validate the table.  It may crash or return
// a garbage result on invalid input.  (However, it won't corrupt memory.)
func CountUnpackedNibblesInSet(src []byte, tablePtr *NibbleLookupTable) int {
	nSrcByte := len(src)
	if nSrcByte < 16 {
		cnt := 0
		for _, srcByte := range src {
			cnt += int(tablePtr.Get(srcByte))
		}
		return cnt
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	return countUnpackedNibblesInSetSSE41Asm(unsafe.Pointer(srcHeader.Data), tablePtr, nSrcByte)
}

// CountUnpackedNibblesInTwoSets counts the number of bytes in src[] which are
// in the given two sets, assuming all bytes are <16.  The sets must be
// represented as table[x] == 1 when value x is in the set, and table[x] == 0
// when x isn't.
//
// WARNING: This function does not validate the tables.  It may crash or return
// garbage results on invalid input.  (However, it won't corrupt memory.)
func CountUnpackedNibblesInTwoSets(src []byte, table1Ptr, table2Ptr *NibbleLookupTable) (int, int) {
	// Building this out now so that biosimd.PackedSeqCountTwo is not a valid
	// reason to stick to packed .bam seq[] representation.
	nSrcByte := len(src)
	cnt2 := 0
	if nSrcByte < 16 {
		cnt1 := 0
		for _, srcByte := range src {
			cnt1 += int(table1Ptr.Get(srcByte))
			cnt2 += int(table2Ptr.Get(srcByte))
		}
		return cnt1, cnt2
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	cnt1 := countUnpackedNibblesInTwoSetsSSE41Asm(&cnt2, unsafe.Pointer(srcHeader.Data), table1Ptr, table2Ptr, nSrcByte)
	return cnt1, cnt2
}

// (could rename Popcnt to Accumulate1 for consistency...)

// Accumulate8 returns the sum of the (unsigned) bytes in src[].
func Accumulate8(src []byte) int {
	nSrcByte := len(src)
	if nSrcByte < 16 {
		cnt := 0
		for _, srcByte := range src {
			cnt += int(srcByte)
		}
		return cnt
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	return accumulate8SSE41Asm(unsafe.Pointer(srcHeader.Data), nSrcByte)
}

// Accumulate8Greater returns the sum of all bytes in src[] greater than the
// given value.
func Accumulate8Greater(src []byte, val byte) int {
	nSrcByte := len(src)
	if nSrcByte < 16 {
		cnt := 0
		for _, srcByte := range src {
			if srcByte > val {
				cnt += int(srcByte)
			}
		}
		return cnt
	}
	srcHeader := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	return accumulate8GreaterSSE41Asm(unsafe.Pointer(srcHeader.Data), int(val), nSrcByte)
}
