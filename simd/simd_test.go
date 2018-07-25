// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package simd_test

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/grailbio/base/simd"
	"github.com/grailbio/testutil/assert"
)

// This is the most-frequently-recommended implementation.  It's decent, so the
// suffix is 'Standard' instead of 'Slow'.
func memset8Standard(dst []byte, val byte) {
	dstLen := len(dst)
	if dstLen != 0 {
		dst[0] = val
		for i := 1; i < dstLen; {
			i += copy(dst[i:], dst[:i])
		}
	}
}

func TestMemset8(t *testing.T) {
	maxSize := 500
	nIter := 200
	main1Arr := simd.MakeUnsafe(maxSize)
	main2Arr := simd.MakeUnsafe(maxSize)
	main3Arr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		main1Slice := main1Arr[sliceStart:sliceEnd]
		main2Slice := main2Arr[sliceStart:sliceEnd]
		main3Slice := main3Arr[sliceStart:sliceEnd]
		byteVal := byte(rand.Intn(256))
		memset8Standard(main1Slice, byteVal)
		simd.Memset8Unsafe(main2Slice, byteVal)
		if !bytes.Equal(main1Slice, main2Slice) {
			t.Fatal("Mismatched Memset8Unsafe result.")
		}
		sentinel := byte(rand.Intn(256))
		if len(main3Slice) > 0 {
			main3Slice[0] = 0
		}
		main3Arr[sliceEnd] = sentinel
		simd.Memset8(main3Slice, byteVal)
		if !bytes.Equal(main1Slice, main3Slice) {
			t.Fatal("Mismatched Memset8 result.")
		}
		if main3Arr[sliceEnd] != sentinel {
			t.Fatal("Memset8 clobbered an extra byte.")
		}
	}
}

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_Memset8/SIMDShort1Cpu-8                     20          62706981 ns/op
Benchmark_Memset8/SIMDShortHalfCpu-8                 100          17559573 ns/op
Benchmark_Memset8/SIMDShortAllCpu-8                  100          17149982 ns/op
Benchmark_Memset8/SIMDLong1Cpu-8                       1        1101524485 ns/op
Benchmark_Memset8/SIMDLongHalfCpu-8                    2         925331938 ns/op
Benchmark_Memset8/SIMDLongAllCpu-8                     2         971422170 ns/op
Benchmark_Memset8/StandardShort1Cpu-8                  5         314689466 ns/op
Benchmark_Memset8/StandardShortHalfCpu-8              20          88260588 ns/op
Benchmark_Memset8/StandardShortAllCpu-8               20          84317546 ns/op
Benchmark_Memset8/StandardLong1Cpu-8                   1        1082736141 ns/op
Benchmark_Memset8/StandardLongHalfCpu-8                2         992904776 ns/op
Benchmark_Memset8/StandardLongAllCpu-8                 1        1052452033 ns/op
Benchmark_Memset8/RangeZeroShort1Cpu-8                30          44907924 ns/op
Benchmark_Memset8/RangeZeroShortHalfCpu-8            100          24173280 ns/op
Benchmark_Memset8/RangeZeroShortAllCpu-8             100          14991003 ns/op
Benchmark_Memset8/RangeZeroLong1Cpu-8                  3         401003587 ns/op
Benchmark_Memset8/RangeZeroLongHalfCpu-8               3         400711072 ns/op
Benchmark_Memset8/RangeZeroLongAllCpu-8                3         404863223 ns/op

Notes: simd.Memset8 is broadly useful for short arrays, though usually a bit
worse than memclr.  However, memclr wins handily in the 249 MB long case on the
test machine, thanks to AVX2 (and, in the AVX2 subroutine, cache-bypassing
stores).
When the simd.Memset8 AVX2 implementation is written, it should obviously
imitate what memclr is doing.
*/

func memset8SimdSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.Memset8(dst, 78)
	}
	return int(dst[0])
}

func memset8StandardSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		memset8Standard(dst, 78)
	}
	return int(dst[0])
}

func memset8RangeZeroSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		// Compiler-recognized loop, which gets converted to a memclr call with
		// fancier optimizations than simd.Memset8.
		for pos := range dst {
			dst[pos] = 0
		}
	}
	return int(dst[0])
}

func Benchmark_Memset8(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   memset8SimdSubtask,
			tag: "SIMD",
		},
		{
			f:   memset8StandardSubtask,
			tag: "Standard",
		},
		{
			f:   memset8RangeZeroSubtask,
			tag: "RangeZero",
		},
	}
	for _, f := range funcs {
		// Base sequence in length-150 .bam read occupies 75 bytes, so 75 is a good
		// size for the short-array benchmark.
		multiBenchmark(f.f, f.tag+"Short", 75, 0, 9999999, b)
		// GRCh37 chromosome 1 length is 249250621, so that's a plausible
		// long-array use case.
		multiBenchmark(f.f, f.tag+"Long", 249250621, 0, 50, b)
	}
}

// This only matches UnpackedNibbleLookupInplace when all bytes < 128; the test
// has been restricted accordingly.  _mm_shuffle_epi8()'s treatment of bytes >=
// 128 usually isn't relevant.
func unpackedNibbleLookupInplaceSlow(main []byte, tablePtr *simd.NibbleLookupTable) {
	for idx := range main {
		main[idx] = tablePtr.Get(main[idx] & 15)
	}
}

func TestUnpackedNibbleLookup(t *testing.T) {
	maxSize := 500
	nIter := 200
	main1Arr := simd.MakeUnsafe(maxSize)
	main2Arr := simd.MakeUnsafe(maxSize)
	main3Arr := simd.MakeUnsafe(maxSize)
	main4Arr := simd.MakeUnsafe(maxSize)
	main5Arr := simd.MakeUnsafe(maxSize)
	table := simd.MakeNibbleLookupTable([16]byte{0, 1, 0, 2, 8, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0})
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		main1Slice := main1Arr[sliceStart:sliceEnd]
		for ii := range main1Slice {
			main1Slice[ii] = byte(rand.Intn(128))
		}
		main2Slice := main2Arr[sliceStart:sliceEnd]
		main3Slice := main3Arr[sliceStart:sliceEnd]
		main4Slice := main4Arr[sliceStart:sliceEnd]
		main5Slice := main5Arr[sliceStart:sliceEnd]

		simd.UnpackedNibbleLookupUnsafe(main3Slice, main1Slice, &table)

		sentinel := byte(rand.Intn(256))
		main4Arr[sliceEnd] = sentinel
		simd.UnpackedNibbleLookup(main4Slice, main1Slice, &table)

		copy(main2Slice, main1Slice)
		copy(main5Slice, main1Slice)

		unpackedNibbleLookupInplaceSlow(main1Slice, &table)
		simd.UnpackedNibbleLookupUnsafeInplace(main2Slice, &table)
		if !bytes.Equal(main1Slice, main2Slice) {
			t.Fatal("Mismatched UnpackedNibbleLookupUnsafeInplace result.")
		}
		if !bytes.Equal(main1Slice, main3Slice) {
			t.Fatal("Mismatched UnpackedNibbleLookupUnsafe result.")
		}
		if !bytes.Equal(main1Slice, main4Slice) {
			t.Fatal("Mismatched UnpackedNibbleLookup result.")
		}
		if main4Arr[sliceEnd] != sentinel {
			t.Fatal("UnpackedNibbleLookup clobbered an extra byte.")
		}

		main5Arr[sliceEnd] = sentinel
		simd.UnpackedNibbleLookupInplace(main5Slice, &table)
		if !bytes.Equal(main1Slice, main5Slice) {
			t.Fatal("Mismatched UnpackedNibbleLookupInplace result.")
		}
		if main5Arr[sliceEnd] != sentinel {
			t.Fatal("UnpackedNibbleLookupInplace clobbered an extra byte.")
		}
	}
}

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_UnpackedNibbleLookupInplace/SIMDShort1Cpu-8                 20         76720863 ns/op
Benchmark_UnpackedNibbleLookupInplace/SIMDShortHalfCpu-8              50         22968008 ns/op
Benchmark_UnpackedNibbleLookupInplace/SIMDShortAllCpu-8              100         18896633 ns/op
Benchmark_UnpackedNibbleLookupInplace/SIMDLong1Cpu-8                   1       1046243684 ns/op
Benchmark_UnpackedNibbleLookupInplace/SIMDLongHalfCpu-8                2        861622838 ns/op
Benchmark_UnpackedNibbleLookupInplace/SIMDLongAllCpu-8                 2        944384349 ns/op
Benchmark_UnpackedNibbleLookupInplace/SlowShort1Cpu-8                  2        532267799 ns/op
Benchmark_UnpackedNibbleLookupInplace/SlowShortHalfCpu-8              10        144993320 ns/op
Benchmark_UnpackedNibbleLookupInplace/SlowShortAllCpu-8               10        146218387 ns/op
Benchmark_UnpackedNibbleLookupInplace/SlowLong1Cpu-8                   1       7745668548 ns/op
Benchmark_UnpackedNibbleLookupInplace/SlowLongHalfCpu-8                1       2169127851 ns/op
Benchmark_UnpackedNibbleLookupInplace/SlowLongAllCpu-8                 1       2164900359 ns/op
*/

func unpackedNibbleLookupInplaceSimdSubtask(dst, src []byte, nIter int) int {
	table := simd.MakeNibbleLookupTable([16]byte{0, 1, 0, 2, 8, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0})
	for iter := 0; iter < nIter; iter++ {
		// Note that this uses the result of one lookup operation as the input to
		// the next.
		// (Given the current table, all values should be 1 or 0 after 3 or more
		// iterations.)
		simd.UnpackedNibbleLookupInplace(dst, &table)
	}
	return int(dst[0])
}

func unpackedNibbleLookupInplaceSlowSubtask(dst, src []byte, nIter int) int {
	table := simd.MakeNibbleLookupTable([16]byte{0, 1, 0, 2, 8, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0})
	for iter := 0; iter < nIter; iter++ {
		unpackedNibbleLookupInplaceSlow(dst, &table)
	}
	return int(dst[0])
}

func Benchmark_UnpackedNibbleLookupInplace(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   unpackedNibbleLookupInplaceSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   unpackedNibbleLookupInplaceSlowSubtask,
			tag: "Slow",
		},
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Short", 75, 0, 9999999, b)
		multiBenchmark(f.f, f.tag+"Long", 249250621, 0, 50, b)
	}
}

func packedNibbleLookupSlow(dst, src []byte, tablePtr *simd.NibbleLookupTable) {
	dstLen := len(dst)
	nSrcFullByte := dstLen / 2
	srcOdd := dstLen & 1
	for srcPos := 0; srcPos < nSrcFullByte; srcPos++ {
		srcByte := src[srcPos]
		dst[2*srcPos] = tablePtr.Get(srcByte & 15)
		dst[2*srcPos+1] = tablePtr.Get(srcByte >> 4)
	}
	if srcOdd == 1 {
		srcByte := src[nSrcFullByte]
		dst[2*nSrcFullByte] = tablePtr.Get(srcByte & 15)
	}
}

func TestPackedNibbleLookup(t *testing.T) {
	maxDstSize := 500
	maxSrcSize := (maxDstSize + 1) / 2
	nIter := 200
	srcArr := simd.MakeUnsafe(maxSrcSize)
	dst1Arr := simd.MakeUnsafe(maxDstSize)
	dst2Arr := simd.MakeUnsafe(maxDstSize)
	table := simd.MakeNibbleLookupTable([16]byte{0, 1, 0, 2, 8, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0})
	for iter := 0; iter < nIter; iter++ {
		srcSliceStart := rand.Intn(maxSrcSize)
		dstSliceStart := srcSliceStart * 2
		dstSliceEnd := dstSliceStart + rand.Intn(maxDstSize-dstSliceStart)
		srcSliceEnd := (dstSliceEnd + 1) / 2
		srcSlice := srcArr[srcSliceStart:srcSliceEnd]
		for ii := range srcSlice {
			srcSlice[ii] = byte(rand.Intn(256))
		}
		dst1Slice := dst1Arr[dstSliceStart:dstSliceEnd]
		dst2Slice := dst2Arr[dstSliceStart:dstSliceEnd]
		packedNibbleLookupSlow(dst1Slice, srcSlice, &table)
		simd.PackedNibbleLookupUnsafe(dst2Slice, srcSlice, &table)
		if !bytes.Equal(dst1Slice, dst2Slice) {
			t.Fatal("Mismatched PackedNibbleLookupUnsafe result.")
		}
		// ack, missed a PackedNibbleLookup bug: it didn't write some of the last
		// few bytes in some cases, but that went undetected because the previous
		// PackedNibbleLookupUnsafe call pre-filled those bytes correctly.
		simd.Memset8Unsafe(dst2Arr, 0)
		sentinel := byte(rand.Intn(256))
		dst2Arr[dstSliceEnd] = sentinel
		simd.PackedNibbleLookup(dst2Slice, srcSlice, &table)
		if !bytes.Equal(dst1Slice, dst2Slice) {
			t.Fatal("Mismatched PackedNibbleLookup result.")
		}
		if dst2Arr[dstSliceEnd] != sentinel {
			t.Fatal("PackedNibbleLookup clobbered an extra byte.")
		}
	}
}

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_PackedNibbleLookup/UnsafeShort1Cpu-8                10         143501956 ns/op
Benchmark_PackedNibbleLookup/UnsafeShortHalfCpu-8             30          38748958 ns/op
Benchmark_PackedNibbleLookup/UnsafeShortAllCpu-8              50          31982398 ns/op
Benchmark_PackedNibbleLookup/UnsafeLong1Cpu-8                  1        1372142640 ns/op
Benchmark_PackedNibbleLookup/UnsafeLongHalfCpu-8               1        1236198290 ns/op
Benchmark_PackedNibbleLookup/UnsafeLongAllCpu-8                1        1265315746 ns/op
Benchmark_PackedNibbleLookup/SIMDShort1Cpu-8                  10         158155872 ns/op
Benchmark_PackedNibbleLookup/SIMDShortHalfCpu-8               30          43098347 ns/op
Benchmark_PackedNibbleLookup/SIMDShortAllCpu-8                30          37593692 ns/op
Benchmark_PackedNibbleLookup/SIMDLong1Cpu-8                    1        1407559630 ns/op
Benchmark_PackedNibbleLookup/SIMDLongHalfCpu-8                 1        1244569913 ns/op
Benchmark_PackedNibbleLookup/SIMDLongAllCpu-8                  1        1245648867 ns/op
Benchmark_PackedNibbleLookup/SlowShort1Cpu-8                   1        1322739228 ns/op
Benchmark_PackedNibbleLookup/SlowShortHalfCpu-8                3         381551545 ns/op
Benchmark_PackedNibbleLookup/SlowShortAllCpu-8                 3         361846656 ns/op
Benchmark_PackedNibbleLookup/SlowLong1Cpu-8                    1        9990188206 ns/op
Benchmark_PackedNibbleLookup/SlowLongHalfCpu-8                 1        2855687759 ns/op
Benchmark_PackedNibbleLookup/SlowLongAllCpu-8                  1        2877628266 ns/op

Notes: Unsafe version of this function is also benchmarked, since the
short-array safety penalty is a bit high here.  This is mainly an indicator of
room for improvement in the safe function; I think it's clear at this point
that we'll probably never need to use the Unsafe interface.
*/

func packedNibbleLookupUnsafeSubtask(dst, src []byte, nIter int) int {
	table := simd.MakeNibbleLookupTable([16]byte{0, 1, 0, 2, 8, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0})
	for iter := 0; iter < nIter; iter++ {
		simd.PackedNibbleLookupUnsafe(dst, src, &table)
	}
	return int(dst[0])
}

func packedNibbleLookupSimdSubtask(dst, src []byte, nIter int) int {
	table := simd.MakeNibbleLookupTable([16]byte{0, 1, 0, 2, 8, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0})
	for iter := 0; iter < nIter; iter++ {
		simd.PackedNibbleLookup(dst, src, &table)
	}
	return int(dst[0])
}

func packedNibbleLookupSlowSubtask(dst, src []byte, nIter int) int {
	table := simd.MakeNibbleLookupTable([16]byte{0, 1, 0, 2, 8, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0})
	for iter := 0; iter < nIter; iter++ {
		packedNibbleLookupSlow(dst, src, &table)
	}
	return int(dst[0])
}

func Benchmark_PackedNibbleLookup(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   packedNibbleLookupUnsafeSubtask,
			tag: "Unsafe",
		},
		{
			f:   packedNibbleLookupSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   packedNibbleLookupSlowSubtask,
			tag: "Slow",
		},
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Short", 150, 75, 9999999, b)
		multiBenchmark(f.f, f.tag+"Long", 249250621, 249250622/2, 50, b)
	}
}

func interleaveSlow(dst, even, odd []byte) {
	dstLen := len(dst)
	evenLen := (dstLen + 1) >> 1
	oddLen := dstLen >> 1
	for idx, oddByte := range odd {
		dst[2*idx] = even[idx]
		dst[2*idx+1] = oddByte
	}
	if oddLen != evenLen {
		dst[oddLen*2] = even[oddLen]
	}
}

func TestInterleave(t *testing.T) {
	maxSrcSize := 500
	maxDstSize := 2 * maxSrcSize
	nIter := 200
	evenArr := simd.MakeUnsafe(maxSrcSize)
	oddArr := simd.MakeUnsafe(maxSrcSize)
	dst1Arr := simd.MakeUnsafe(maxDstSize)
	dst2Arr := simd.MakeUnsafe(maxDstSize)
	for iter := 0; iter < nIter; iter++ {
		srcSliceStart := rand.Intn(maxSrcSize)
		dstSliceStart := srcSliceStart * 2
		dstSliceEnd := dstSliceStart + rand.Intn(maxDstSize-dstSliceStart)
		evenSliceEnd := (dstSliceEnd + 1) >> 1
		oddSliceEnd := dstSliceEnd >> 1
		evenSlice := evenArr[srcSliceStart:evenSliceEnd]
		oddSlice := oddArr[srcSliceStart:oddSliceEnd]
		for ii := range evenSlice {
			evenSlice[ii] = byte(rand.Intn(256))
		}
		for ii := range oddSlice {
			oddSlice[ii] = byte(rand.Intn(256))
		}
		dst1Slice := dst1Arr[dstSliceStart:dstSliceEnd]
		dst2Slice := dst2Arr[dstSliceStart:dstSliceEnd]
		interleaveSlow(dst1Slice, evenSlice, oddSlice)
		simd.Interleave8Unsafe(dst2Slice, evenSlice, oddSlice)
		if !bytes.Equal(dst1Slice, dst2Slice) {
			t.Fatal("Mismatched Interleave8Unsafe result.")
		}
		sentinel := byte(rand.Intn(256))
		dst2Arr[dstSliceEnd] = sentinel
		simd.Interleave8(dst2Slice, evenSlice, oddSlice)
		if !bytes.Equal(dst1Slice, dst2Slice) {
			t.Fatal("Mismatched Interleave8 result.")
		}
		if dst2Arr[dstSliceEnd] != sentinel {
			t.Fatal("Interleave8 clobbered an extra byte.")
		}
	}
}

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_Interleave/UnsafeShort1Cpu-8                10         124397567 ns/op
Benchmark_Interleave/UnsafeShortHalfCpu-8             50          33427370 ns/op
Benchmark_Interleave/UnsafeShortAllCpu-8              50          27522495 ns/op
Benchmark_Interleave/UnsafeLong1Cpu-8                  1        1364788736 ns/op
Benchmark_Interleave/UnsafeLongHalfCpu-8               1        1194034677 ns/op
Benchmark_Interleave/UnsafeLongAllCpu-8                1        1240540994 ns/op
Benchmark_Interleave/SIMDShort1Cpu-8                  10         143574503 ns/op
Benchmark_Interleave/SIMDShortHalfCpu-8               30          40429942 ns/op
Benchmark_Interleave/SIMDShortAllCpu-8                50          30500450 ns/op
Benchmark_Interleave/SIMDLong1Cpu-8                    1        1281952758 ns/op
Benchmark_Interleave/SIMDLongHalfCpu-8                 1        1210134670 ns/op
Benchmark_Interleave/SIMDLongAllCpu-8                  1        1284786977 ns/op
Benchmark_Interleave/SlowShort1Cpu-8                   2         880545817 ns/op
Benchmark_Interleave/SlowShortHalfCpu-8                5         234673823 ns/op
Benchmark_Interleave/SlowShortAllCpu-8                 5         230332535 ns/op
Benchmark_Interleave/SlowLong1Cpu-8                    1        6669283712 ns/op
Benchmark_Interleave/SlowLongHalfCpu-8                 1        1860713287 ns/op
Benchmark_Interleave/SlowLongAllCpu-8                  1        1807886977 ns/op
*/

func interleaveUnsafeSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.Interleave8Unsafe(dst, src, src)
	}
	return int(dst[0])
}

func interleaveSimdSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.Interleave8(dst, src, src)
	}
	return int(dst[0])
}

func interleaveSlowSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		interleaveSlow(dst, src, src)
	}
	return int(dst[0])
}

func Benchmark_Interleave(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   interleaveUnsafeSubtask,
			tag: "Unsafe",
		},
		{
			f:   interleaveSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   interleaveSlowSubtask,
			tag: "Slow",
		},
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Short", 150, 75, 9999999, b)
		multiBenchmark(f.f, f.tag+"Long", 124625311*2, 124625311, 50, b)
	}
}

func reverse8Slow(main []byte) {
	nByte := len(main)
	nByteDiv2 := nByte >> 1
	for idx, invIdx := 0, nByte-1; idx != nByteDiv2; idx, invIdx = idx+1, invIdx-1 {
		main[idx], main[invIdx] = main[invIdx], main[idx]
	}
}

func TestReverse8(t *testing.T) {
	maxSize := 500
	nIter := 200
	main1Arr := simd.MakeUnsafe(maxSize)
	main2Arr := simd.MakeUnsafe(maxSize)
	main3Arr := simd.MakeUnsafe(maxSize)
	main4Arr := simd.MakeUnsafe(maxSize)
	src2Arr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		main1Slice := main1Arr[sliceStart:sliceEnd]
		main2Slice := main2Arr[sliceStart:sliceEnd]
		main3Slice := main3Arr[sliceStart:sliceEnd]
		main4Slice := main4Arr[sliceStart:sliceEnd]
		src2Slice := src2Arr[sliceStart:sliceEnd]
		for ii := range main1Slice {
			main1Slice[ii] = byte(rand.Intn(256))
		}
		copy(main2Slice, main1Slice)
		copy(src2Slice, main1Slice)
		sentinel := byte(rand.Intn(256))
		main2Arr[sliceEnd] = sentinel
		main4Arr[sliceEnd] = sentinel
		simd.Reverse8Unsafe(main3Slice, main1Slice)
		simd.Reverse8(main4Slice, main1Slice)
		reverse8Slow(main1Slice)
		simd.Reverse8Inplace(main2Slice)
		if !bytes.Equal(main1Slice, main2Slice) {
			t.Fatal("Mismatched Reverse8Inplace result.")
		}
		if main2Arr[sliceEnd] != sentinel {
			t.Fatal("Reverse8Inplace clobbered an extra byte.")
		}
		if !bytes.Equal(main1Slice, main3Slice) {
			t.Fatal("Mismatched Reverse8Unsafe result.")
		}
		if !bytes.Equal(main1Slice, main4Slice) {
			t.Fatal("Mismatched Reverse8 result.")
		}
		if main4Arr[sliceEnd] != sentinel {
			t.Fatal("Reverse8 clobbered an extra byte.")
		}
		simd.Reverse8Inplace(main4Slice)
		if !bytes.Equal(src2Slice, main4Slice) {
			t.Fatal("Reverse8Inplace didn't invert itself.")
		}
	}
}

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_Reverse8Inplace/SIMDShort1Cpu-8                     20          67121510 ns/op
Benchmark_Reverse8Inplace/SIMDShortHalfCpu-8                 100          18891965 ns/op
Benchmark_Reverse8Inplace/SIMDShortAllCpu-8                  100          16177224 ns/op
Benchmark_Reverse8Inplace/SIMDLong1Cpu-8                       1        1115497033 ns/op
Benchmark_Reverse8Inplace/SIMDLongHalfCpu-8                    2         885764257 ns/op
Benchmark_Reverse8Inplace/SIMDLongAllCpu-8                     2         941948715 ns/op
Benchmark_Reverse8Inplace/SlowShort1Cpu-8                      3         398662666 ns/op
Benchmark_Reverse8Inplace/SlowShortHalfCpu-8                  10         105618119 ns/op
Benchmark_Reverse8Inplace/SlowShortAllCpu-8                   10         184808267 ns/op
Benchmark_Reverse8Inplace/SlowLong1Cpu-8                       1        5665556658 ns/op
Benchmark_Reverse8Inplace/SlowLongHalfCpu-8                    1        1597487158 ns/op
Benchmark_Reverse8Inplace/SlowLongAllCpu-8                     1        1616963854 ns/op
*/

func reverse8InplaceSimdSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.Reverse8Inplace(dst)
	}
	return int(dst[0])
}

func reverse8InplaceSlowSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		reverse8Slow(dst)
	}
	return int(dst[0])
}

func Benchmark_Reverse8Inplace(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   reverse8InplaceSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   reverse8InplaceSlowSubtask,
			tag: "Slow",
		},
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Short", 75, 0, 9999999, b)
		multiBenchmark(f.f, f.tag+"Long", 249250621, 0, 50, b)
	}
}

func bitFromEveryByteSlow(dst, src []byte, bitIdx int) {
	requiredDstLen := (len(src) + 7) >> 3
	if (len(dst) < requiredDstLen) || (uint(bitIdx) > 7) {
		panic("BitFromEveryByte requires len(dst) >= (len(src) + 7) / 8 and 0 <= bitIdx < 8.")
	}
	dst = dst[:requiredDstLen]
	for i := range dst {
		dst[i] = 0
	}
	for i, b := range src {
		dst[i>>3] |= ((b >> uint32(bitIdx)) & 1) << uint32(i&7)
	}
}

func bitFromEveryByteFancyNoasm(dst, src []byte, bitIdx int) {
	requiredDstLen := (len(src) + 7) >> 3
	if (len(dst) < requiredDstLen) || (uint(bitIdx) > 7) {
		panic("BitFromEveryByte requires len(dst) >= (len(src) + 7) / 8 and 0 <= bitIdx < 8.")
	}
	nSrcFullWord := len(src) >> 3
	for i := 0; i < nSrcFullWord; i++ {
		// Tried using a unsafeBytesToWords function on src in place of
		// binary.LittleEndian.Uint64, and it barely made any difference.
		srcWord := binary.LittleEndian.Uint64(src[i*8:i*8+8]) >> uint32(bitIdx)

		srcWord &= 0x101010101010101

		// Before this operation, the bits of interest are at positions 0, 8, 16,
		// 24, 32, 40, 48, and 56 in srcWord, and all other bits are guaranteed to
		// be zero.
		//
		// Suppose the bit at position 16 is set, and no other bits are set.  What
		// does multiplication by the magic number 0x102040810204080 accomplish?
		// Well, the magic number has bits set at positions 7, 14, 21, 28, 35, 42,
		// 49, and 56.  Multiplying by 2^16 is equivalent to left-shifting by 16,
		// so the product has bits set at positions (7+16), (14+16), (21+16),
		// (28+16), (35+16), (42+16), and the last two overflow off the top end.
		//
		// Now suppose the bits at position 0 and 16 are both set.  The result is
		// then the sum of (2^0) * <magic number> + (2^16) * <magic number>.  The
		// first term in this sum has bits set at positions 7, 14, ..., 56.
		// Critically, *none of these bits overlap with the second term*, so there
		// are no 'carries' when we add the two terms together.  So the final
		// product has bits set at positions 7, 14, 21, 23, 28, 30, 35, 37, 42, 44,
		// 49, 51, 56, and 58.
		//
		// It turns out that none of the bits in any of the 8 terms of this product
		// have overlapping positions.  So the multiplication operation just makes
		// a bunch of left-shifted copies of the original bits... and in
		// particular, bits 56-63 of the product are:
		//   56: original bit 0, left-shifted 56
		//   57: original bit 8, left-shifted 49
		//   58: original bit 16, left-shifted 42
		//   59: original bit 24, left-shifted 35
		//   60: original bit 32, left-shifted 28
		//   61: original bit 40, left-shifted 21
		//   62: original bit 48, left-shifted 14
		//   63: original bit 56, left-shifted 7
		// Thus, right-shifting the product by 56 gives us the byte we want.
		//
		// This is a very esoteric algorithm, and it doesn't have much direct
		// application because all 64-bit x86 processors provide an assembly
		// instruction which lets you do this >6 times as quickly.  Occasionally
		// the idea of using multiplication to create staggered left-shifted copies
		// of bits does genuinely come in handy, though.
		dst[i] = byte((srcWord * 0x102040810204080) >> 56)
	}
	if nSrcFullWord != requiredDstLen {
		srcLast := src[nSrcFullWord*8:]
		dstLast := dst[nSrcFullWord:requiredDstLen]
		for i := range dstLast {
			dstLast[i] = 0
		}
		for i, b := range srcLast {
			dstLast[i>>3] |= ((b >> uint32(bitIdx)) & 1) << uint32(i&7)
		}
	}
}

func TestBitFromEveryByte(t *testing.T) {
	maxSize := 500
	nIter := 200
	rand.Seed(1)
	srcArr := make([]byte, maxSize)
	dstArr1 := make([]byte, maxSize)
	dstArr2 := make([]byte, maxSize)
	dstArr3 := make([]byte, maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		srcSize := rand.Intn(maxSize - sliceStart)
		srcSliceEnd := sliceStart + srcSize
		srcSlice := srcArr[sliceStart:srcSliceEnd]

		minDstSize := (srcSize + 7) >> 3
		dstSliceEnd := sliceStart + minDstSize
		dstSlice1 := dstArr1[sliceStart:dstSliceEnd]
		dstSlice2 := dstArr2[sliceStart:dstSliceEnd]
		dstSlice3 := dstArr3[sliceStart:dstSliceEnd]

		for ii := range srcSlice {
			srcSlice[ii] = byte(rand.Intn(256))
		}
		sentinel := byte(rand.Intn(256))
		dstArr2[dstSliceEnd] = sentinel

		bitIdx := rand.Intn(8)
		bitFromEveryByteSlow(dstSlice1, srcSlice, bitIdx)
		simd.BitFromEveryByte(dstSlice2, srcSlice, bitIdx)
		assert.EQ(t, dstSlice1, dstSlice2)
		assert.EQ(t, sentinel, dstArr2[dstSliceEnd])

		// Also validate the assembly-free multiplication-based algorithm.
		sentinel = byte(rand.Intn(256))
		dstArr3[dstSliceEnd] = sentinel
		bitFromEveryByteFancyNoasm(dstSlice3, srcSlice, bitIdx)
		assert.EQ(t, dstSlice1, dstSlice3)
		assert.EQ(t, sentinel, dstArr3[dstSliceEnd])
	}
}

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_BitFromEveryByte/SIMDLong1Cpu-8                    200           6861450 ns/op
Benchmark_BitFromEveryByte/SIMDLongHalfCpu-8                 200           7360937 ns/op
Benchmark_BitFromEveryByte/SIMDLongAllCpu-8                  200           8846261 ns/op
Benchmark_BitFromEveryByte/FancyNoasmLong1Cpu-8               20          58756902 ns/op
Benchmark_BitFromEveryByte/FancyNoasmLongHalfCpu-8                   100         17244847 ns/op
Benchmark_BitFromEveryByte/FancyNoasmLongAllCpu-8                    100         16624282 ns/op
Benchmark_BitFromEveryByte/SlowLong1Cpu-8                              3        422073091 ns/op
Benchmark_BitFromEveryByte/SlowLongHalfCpu-8                          10        117732813 ns/op
Benchmark_BitFromEveryByte/SlowLongAllCpu-8                           10        114903556 ns/op

Notes: 1Cpu has higher throughput than HalfCpu/AllCpu on this test machine due
to L3 cache saturation: multiBenchmarkDstSrc makes each goroutine process its
own ~4 MB job, rather than splitting a single job into smaller pieces, and a
15-inch 2016 MacBook Pro has a 8 MB L3 cache.  If you shrink the test size to
len(src)=400000, HalfCpu outperforms 1Cpu by the expected amount.

I'm leaving this unusual benchmark result here since (i) it corresponds to how
we actually need to use the function, and (ii) this phenomenon is definitely
worth knowing about.
*/

func bitFromEveryByteSimdSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.BitFromEveryByte(dst, src, 0)
	}
	return int(dst[0])
}

func bitFromEveryByteFancyNoasmSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		bitFromEveryByteFancyNoasm(dst, src, 0)
	}
	return int(dst[0])
}

func bitFromEveryByteSlowSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		bitFromEveryByteSlow(dst, src, 0)
	}
	return int(dst[0])
}

func Benchmark_BitFromEveryByte(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   bitFromEveryByteSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   bitFromEveryByteFancyNoasmSubtask,
			tag: "FancyNoasm",
		},
		{
			f:   bitFromEveryByteSlowSubtask,
			tag: "Slow",
		},
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Long", 4091904/8, 4091904, 50, b)
	}
}
