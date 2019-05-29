// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package simd_test

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"runtime"
	"testing"

	"github.com/grailbio/base/simd"
	"github.com/grailbio/testutil/assert"
)

/*
Initial benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_Memset8Short1-8             20          56542414 ns/op
Benchmark_Memset8Short4-8            100          15969877 ns/op
Benchmark_Memset8ShortMax-8          100          15780214 ns/op
Benchmark_Memset8Long1-8               1        1279094415 ns/op
Benchmark_Memset8Long4-8               1        1902840097 ns/op
Benchmark_Memset8LongMax-8             1        2715013574 ns/op

Benchmark_UnpackedNibbleLookupShort1-8                20          66268257 ns/op
Benchmark_UnpackedNibbleLookupShort4-8               100          18575755 ns/op
Benchmark_UnpackedNibbleLookupShortMax-8             100          17474281 ns/op
Benchmark_UnpackedNibbleLookupLong1-8                  1        1330878965 ns/op
Benchmark_UnpackedNibbleLookupLong4-8                  1        1977241995 ns/op
Benchmark_UnpackedNibbleLookupLongMax-8                1        2793933818 ns/op

Benchmark_PackedNibbleLookupShort1-8                  20          80579763 ns/op
Benchmark_PackedNibbleLookupShort4-8                 100          23488681 ns/op
Benchmark_PackedNibbleLookupShortMax-8               100          21701360 ns/op
Benchmark_PackedNibbleLookupLong1-8                    1        1470408074 ns/op
Benchmark_PackedNibbleLookupLong4-8                    1        2103843655 ns/op
Benchmark_PackedNibbleLookupLongMax-8                  1        2767976716 ns/op

Benchmark_InterleaveShort1-8                  10         122320311 ns/op
Benchmark_InterleaveShort4-8                  50          33240437 ns/op
Benchmark_InterleaveShortMax-8                50          27383249 ns/op
Benchmark_InterleaveLong1-8                    1        1557992496 ns/op
Benchmark_InterleaveLong4-8                    1        2177311837 ns/op
Benchmark_InterleaveLongMax-8                  1        2838302958 ns/op

Benchmark_Reverse8Short1-8            20          66878761 ns/op
Benchmark_Reverse8Short4-8           100          18888361 ns/op
Benchmark_Reverse8ShortMax-8         100          17845626 ns/op
Benchmark_Reverse8Long1-8              1        1274790843 ns/op
Benchmark_Reverse8Long4-8              1        1962669700 ns/op
Benchmark_Reverse8LongMax-8            1        2719838443 ns/op

Benchmark_BitFromEveryByte-8                         200           7089305 ns/op
Benchmark_BitFromEveryByteFancyNoasm-8                20          58323674 ns/op
Benchmark_BitFromEveryByteSlow-8                       3         418394417 ns/op


For comparison, memset8:
Benchmark_Memset8Short1-8               5         270933107 ns/op
Benchmark_Memset8Short4-8              20          78389931 ns/op
Benchmark_Memset8ShortMax-8            20          66983738 ns/op
Benchmark_Memset8Long1-8                1        1342542739 ns/op
Benchmark_Memset8Long4-8                1        1944395002 ns/op
Benchmark_Memset8LongMax-8              1        2757737157 ns/op

memset-to-zero range for loop:
Benchmark_Memset8Short1-8             30          37976858 ns/op
Benchmark_Memset8Short4-8             50          25033805 ns/op
Benchmark_Memset8ShortMax-8          100          14801649 ns/op
Benchmark_Memset8Long1-8               3         448067523 ns/op
Benchmark_Memset8Long4-8               1        1361988705 ns/op
Benchmark_Memset8LongMax-8             1        2126505354 ns/op
(Note that this is usually better than simd.Memset8.  This is due to reduced
function call overhead and use of AVX2 (with cache-bypassing stores in the AVX2
>32 MiB case); there was no advantage to replacing simd.Memset8 with the
non-AVX2 portion of runtime.memclr_amd64.)

unpackedNibbleLookupInplaceSlow (&15 removed, bytes restricted to 0..15):
Benchmark_UnpackedNibbleLookupShort1-8                 2         524170511 ns/op
Benchmark_UnpackedNibbleLookupShort4-8                10         147371412 ns/op
Benchmark_UnpackedNibbleLookupShortMax-8              10         142252262 ns/op
Benchmark_UnpackedNibbleLookupLong1-8                  1        8123456605 ns/op
Benchmark_UnpackedNibbleLookupLong4-8                  1        5069456472 ns/op
Benchmark_UnpackedNibbleLookupLongMax-8                1        3929059263 ns/op

packedNibbleLookupSlow:
Benchmark_PackedNibbleLookupShort1-8                   2         572680365 ns/op
Benchmark_PackedNibbleLookupShort4-8                  10         158619127 ns/op
Benchmark_PackedNibbleLookupShortMax-8                10         155940159 ns/op
Benchmark_PackedNibbleLookupLong1-8                    1        8956157310 ns/op
Benchmark_PackedNibbleLookupLong4-8                    1        3226223964 ns/op
Benchmark_PackedNibbleLookupLongMax-8                  1        3788519710 ns/op

interleaveSlow:
Benchmark_InterleaveShort1-8                   2         779212342 ns/op
Benchmark_InterleaveShort4-8                   5         207224364 ns/op
Benchmark_InterleaveShortMax-8                 5         213528353 ns/op
Benchmark_InterleaveLong1-8                    1        6926143664 ns/op
Benchmark_InterleaveLong4-8                    1        2745455753 ns/op
Benchmark_InterleaveLongMax-8                  1        3664858002 ns/op

reverseSlow:
Benchmark_Reverse8Short1-8              3         423063894 ns/op
Benchmark_Reverse8Short4-8             10         112274707 ns/op
Benchmark_Reverse8ShortMax-8           10         196379771 ns/op
Benchmark_Reverse8Long1-8               1        6270445876 ns/op
Benchmark_Reverse8Long4-8               1        3965932146 ns/op
Benchmark_Reverse8LongMax-8             1        3349559784 ns/op
*/

func memset8Subtask(args interface{}, nIter int) int {
	a := args.(dstSrcArgs)
	for iter := 0; iter < nIter; iter++ {
		// Compiler-recognized range-for loop, for comparison.
		/*
			for pos := range dst {
				dst[pos] = 0
			}
		*/
		simd.Memset8Unsafe(a.dst, 78)
	}
	return int(a.dst[0])
}

// Base sequence in length-150 .bam read occupies 75 bytes, so 75 is a good
// size for the short-array benchmark.
func Benchmark_Memset8Short1(b *testing.B) {
	multiBenchmarkDstSrc(memset8Subtask, 1, 75, 0, 9999999, b)
}

func Benchmark_Memset8Short4(b *testing.B) {
	multiBenchmarkDstSrc(memset8Subtask, 4, 75, 0, 9999999, b)
}

func Benchmark_Memset8ShortMax(b *testing.B) {
	multiBenchmarkDstSrc(memset8Subtask, runtime.NumCPU(), 75, 0, 9999999, b)
}

// GRCh37 chromosome 1 length is 249250621, so that's a plausible long-array
// use case.
func Benchmark_Memset8Long1(b *testing.B) {
	multiBenchmarkDstSrc(memset8Subtask, 1, 249250621, 0, 50, b)
}

func Benchmark_Memset8Long4(b *testing.B) {
	multiBenchmarkDstSrc(memset8Subtask, 4, 249250621, 0, 50, b)
}

func Benchmark_Memset8LongMax(b *testing.B) {
	multiBenchmarkDstSrc(memset8Subtask, runtime.NumCPU(), 249250621, 0, 50, b)
}

func memset8(dst []byte, val byte) {
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
		memset8(main1Slice, byteVal)
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

func unpackedNibbleLookupSubtask(args interface{}, nIter int) int {
	a := args.(dstSrcArgs)
	table := simd.MakeNibbleLookupTable([16]byte{0, 1, 0, 2, 8, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0})
	for iter := 0; iter < nIter; iter++ {
		// Note that this uses the result of one lookup operation as the input to
		// the next.
		// (Given the current table, all values should be 1 or 0 after 3 or more
		// iterations.)
		simd.UnpackedNibbleLookupUnsafeInplace(a.dst, &table)
	}
	return int(a.dst[0])
}

func Benchmark_UnpackedNibbleLookupShort1(b *testing.B) {
	multiBenchmarkDstSrc(unpackedNibbleLookupSubtask, 1, 75, 0, 9999999, b)
}

func Benchmark_UnpackedNibbleLookupShort4(b *testing.B) {
	multiBenchmarkDstSrc(unpackedNibbleLookupSubtask, 4, 75, 0, 9999999, b)
}

func Benchmark_UnpackedNibbleLookupShortMax(b *testing.B) {
	multiBenchmarkDstSrc(unpackedNibbleLookupSubtask, runtime.NumCPU(), 75, 0, 9999999, b)
}

func Benchmark_UnpackedNibbleLookupLong1(b *testing.B) {
	multiBenchmarkDstSrc(unpackedNibbleLookupSubtask, 1, 249250621, 0, 50, b)
}

func Benchmark_UnpackedNibbleLookupLong4(b *testing.B) {
	multiBenchmarkDstSrc(unpackedNibbleLookupSubtask, 4, 249250621, 0, 50, b)
}

func Benchmark_UnpackedNibbleLookupLongMax(b *testing.B) {
	multiBenchmarkDstSrc(unpackedNibbleLookupSubtask, runtime.NumCPU(), 249250621, 0, 50, b)
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

func packedNibbleLookupSubtask(args interface{}, nIter int) int {
	a := args.(dstSrcArgs)
	table := simd.MakeNibbleLookupTable([16]byte{0, 1, 0, 2, 8, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0})
	for iter := 0; iter < nIter; iter++ {
		simd.PackedNibbleLookupUnsafe(a.dst, a.src, &table)
	}
	return int(a.dst[0])
}

func Benchmark_PackedNibbleLookupShort1(b *testing.B) {
	multiBenchmarkDstSrc(packedNibbleLookupSubtask, 1, 75, 38, 9999999, b)
}

func Benchmark_PackedNibbleLookupShort4(b *testing.B) {
	multiBenchmarkDstSrc(packedNibbleLookupSubtask, 4, 75, 38, 9999999, b)
}

func Benchmark_PackedNibbleLookupShortMax(b *testing.B) {
	multiBenchmarkDstSrc(packedNibbleLookupSubtask, runtime.NumCPU(), 75, 38, 9999999, b)
}

func Benchmark_PackedNibbleLookupLong1(b *testing.B) {
	multiBenchmarkDstSrc(packedNibbleLookupSubtask, 1, 249250621, 249250622/2, 50, b)
}

func Benchmark_PackedNibbleLookupLong4(b *testing.B) {
	multiBenchmarkDstSrc(packedNibbleLookupSubtask, 4, 249250621, 249250622/2, 50, b)
}

func Benchmark_PackedNibbleLookupLongMax(b *testing.B) {
	multiBenchmarkDstSrc(packedNibbleLookupSubtask, runtime.NumCPU(), 249250621, 249250622/2, 50, b)
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

func interleaveSubtask(args interface{}, nIter int) int {
	a := args.(dstSrcArgs)
	for iter := 0; iter < nIter; iter++ {
		simd.Interleave8Unsafe(a.dst, a.src, a.src)
	}
	return int(a.dst[0])
}

func Benchmark_InterleaveShort1(b *testing.B) {
	multiBenchmarkDstSrc(interleaveSubtask, 1, 150, 75, 9999999, b)
}

func Benchmark_InterleaveShort4(b *testing.B) {
	multiBenchmarkDstSrc(interleaveSubtask, 4, 150, 75, 9999999, b)
}

func Benchmark_InterleaveShortMax(b *testing.B) {
	multiBenchmarkDstSrc(interleaveSubtask, runtime.NumCPU(), 150, 75, 9999999, b)
}

func Benchmark_InterleaveLong1(b *testing.B) {
	multiBenchmarkDstSrc(interleaveSubtask, 1, 124625311*2, 124625311, 50, b)
}

func Benchmark_InterleaveLong4(b *testing.B) {
	multiBenchmarkDstSrc(interleaveSubtask, 4, 124625311*2, 124625311, 50, b)
}

func Benchmark_InterleaveLongMax(b *testing.B) {
	multiBenchmarkDstSrc(interleaveSubtask, runtime.NumCPU(), 124625311*2, 124625311, 50, b)
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

func reverse8Subtask(args interface{}, nIter int) int {
	a := args.(dstSrcArgs)
	for iter := 0; iter < nIter; iter++ {
		simd.Reverse8Inplace(a.dst)
	}
	return int(a.dst[0])
}

func Benchmark_Reverse8Short1(b *testing.B) {
	multiBenchmarkDstSrc(reverse8Subtask, 1, 75, 0, 9999999, b)
}

func Benchmark_Reverse8Short4(b *testing.B) {
	multiBenchmarkDstSrc(reverse8Subtask, 4, 75, 0, 9999999, b)
}

func Benchmark_Reverse8ShortMax(b *testing.B) {
	multiBenchmarkDstSrc(reverse8Subtask, runtime.NumCPU(), 75, 0, 9999999, b)
}

func Benchmark_Reverse8Long1(b *testing.B) {
	multiBenchmarkDstSrc(reverse8Subtask, 1, 249250621, 0, 50, b)
}

func Benchmark_Reverse8Long4(b *testing.B) {
	multiBenchmarkDstSrc(reverse8Subtask, 4, 249250621, 0, 50, b)
}

func Benchmark_Reverse8LongMax(b *testing.B) {
	multiBenchmarkDstSrc(reverse8Subtask, runtime.NumCPU(), 249250621, 0, 50, b)
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

func bitFromEveryByteSubtask(args interface{}, nIter int) int {
	a := args.(dstSrcArgs)
	for iter := 0; iter < nIter; iter++ {
		simd.BitFromEveryByte(a.dst, a.src, 0)
	}
	return int(a.dst[0])
}

func bitFromEveryByteFancyNoasmSubtask(args interface{}, nIter int) int {
	a := args.(dstSrcArgs)
	for iter := 0; iter < nIter; iter++ {
		bitFromEveryByteFancyNoasm(a.dst, a.src, 0)
	}
	return int(a.dst[0])
}

func bitFromEveryByteSlowSubtask(args interface{}, nIter int) int {
	a := args.(dstSrcArgs)
	for iter := 0; iter < nIter; iter++ {
		bitFromEveryByteSlow(a.dst, a.src, 0)
	}
	return int(a.dst[0])
}

func Benchmark_BitFromEveryByte(b *testing.B) {
	multiBenchmarkDstSrc(bitFromEveryByteSubtask, 1, 4091904/8, 4091904, 50, b)
}

func Benchmark_BitFromEveryByteFancyNoasm(b *testing.B) {
	multiBenchmarkDstSrc(bitFromEveryByteFancyNoasmSubtask, 1, 4091904/8, 4091904, 50, b)
}

func Benchmark_BitFromEveryByteSlow(b *testing.B) {
	multiBenchmarkDstSrc(bitFromEveryByteSlowSubtask, 1, 4091904/8, 4091904, 50, b)
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
