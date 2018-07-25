// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package simd_test

import (
	"bytes"
	"math/rand"
	"runtime"
	"testing"

	"github.com/grailbio/base/simd"
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

/*
type QLetter struct {
	L byte
	Q byte
}
*/

func memset8Subtask(dst []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		// Compiler-recognized range-for loop, for comparison.
		/*
			for pos := range dst {
				dst[pos] = 0
			}
		*/
		simd.Memset8Unsafe(dst, 78)
	}
	return int(dst[0])
}

func memset8SubtaskFuture(dst []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- memset8Subtask(dst, nIter) }()
	return future
}

func multiMemset8(dsts [][]byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = memset8SubtaskFuture(dsts[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = memset8SubtaskFuture(dsts[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkMemset8(cpus int, nByte int, nJob int, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	mainSlices := make([][]byte, cpus)
	for ii := range mainSlices {
		// Add 63 to prevent false sharing.
		newArr := simd.MakeUnsafe(nByte + 63)
		for jj := 0; jj < nByte; jj++ {
			newArr[jj] = byte(jj * 3)
		}
		mainSlices[ii] = newArr[:nByte]
	}
	for i := 0; i < b.N; i++ {
		multiMemset8(mainSlices, cpus, nJob)
	}
}

// Base sequence in length-150 .bam read occupies 75 bytes, so 75 is a good
// size for the short-array benchmark.
func Benchmark_Memset8Short1(b *testing.B) {
	benchmarkMemset8(1, 75, 9999999, b)
}

func Benchmark_Memset8Short4(b *testing.B) {
	benchmarkMemset8(4, 75, 9999999, b)
}

func Benchmark_Memset8ShortMax(b *testing.B) {
	benchmarkMemset8(runtime.NumCPU(), 75, 9999999, b)
}

// GRCh37 chromosome 1 length is 249250621, so that's a plausible long-array
// use case.
func Benchmark_Memset8Long1(b *testing.B) {
	benchmarkMemset8(1, 249250621, 50, b)
}

func Benchmark_Memset8Long4(b *testing.B) {
	benchmarkMemset8(4, 249250621, 50, b)
}

func Benchmark_Memset8LongMax(b *testing.B) {
	benchmarkMemset8(runtime.NumCPU(), 249250621, 50, b)
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

func unpackedNibbleLookupSubtask(main []byte, nIter int) int {
	table := [...]byte{0, 1, 0, 2, 8, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0}
	for iter := 0; iter < nIter; iter++ {
		// Note that this uses the result of one lookup operation as the input to
		// the next.
		// (Given the current table, all values should be 1 or 0 after 3 or more
		// iterations.)
		simd.UnpackedNibbleLookupUnsafeInplace(main, &table)
	}
	return int(main[0])
}

func unpackedNibbleLookupSubtaskFuture(main []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- unpackedNibbleLookupSubtask(main, nIter) }()
	return future
}

func multiUnpackedNibbleLookup(mains [][]byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = unpackedNibbleLookupSubtaskFuture(mains[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = unpackedNibbleLookupSubtaskFuture(mains[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkUnpackedNibbleLookup(cpus int, nByte int, nJob int, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	mainSlices := make([][]byte, cpus)
	for ii := range mainSlices {
		// Add 63 to prevent false sharing.
		newArr := simd.MakeUnsafe(nByte + 63)
		for jj := 0; jj < nByte; jj++ {
			newArr[jj] = byte(jj * 3)
		}
		mainSlices[ii] = newArr[:nByte]
	}
	for i := 0; i < b.N; i++ {
		multiUnpackedNibbleLookup(mainSlices, cpus, nJob)
	}
}

func Benchmark_UnpackedNibbleLookupShort1(b *testing.B) {
	benchmarkUnpackedNibbleLookup(1, 75, 9999999, b)
}

func Benchmark_UnpackedNibbleLookupShort4(b *testing.B) {
	benchmarkUnpackedNibbleLookup(4, 75, 9999999, b)
}

func Benchmark_UnpackedNibbleLookupShortMax(b *testing.B) {
	benchmarkUnpackedNibbleLookup(runtime.NumCPU(), 75, 9999999, b)
}

func Benchmark_UnpackedNibbleLookupLong1(b *testing.B) {
	benchmarkUnpackedNibbleLookup(1, 249250621, 50, b)
}

func Benchmark_UnpackedNibbleLookupLong4(b *testing.B) {
	benchmarkUnpackedNibbleLookup(4, 249250621, 50, b)
}

func Benchmark_UnpackedNibbleLookupLongMax(b *testing.B) {
	benchmarkUnpackedNibbleLookup(runtime.NumCPU(), 249250621, 50, b)
}

// This only matches UnpackedNibbleLookupInplace when all bytes < 128; the test
// has been restricted accordingly.  _mm_shuffle_epi8()'s treatment of bytes >=
// 128 usually isn't relevant.
func unpackedNibbleLookupInplaceSlow(main []byte, tablePtr *[16]byte) {
	for idx := range main {
		main[idx] = tablePtr[main[idx]&15]
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
	table := [...]byte{0, 1, 0, 2, 8, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0}
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

func packedNibbleLookupSubtask(dst, src []byte, nIter int) int {
	table := [...]byte{0, 1, 0, 2, 8, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0}
	for iter := 0; iter < nIter; iter++ {
		simd.PackedNibbleLookupUnsafe(dst, src, &table)
	}
	return int(dst[0])
}

func packedNibbleLookupSubtaskFuture(dst, src []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- packedNibbleLookupSubtask(dst, src, nIter) }()
	return future
}

func multiPackedNibbleLookup(dsts, srcs [][]byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = packedNibbleLookupSubtaskFuture(dsts[taskIdx], srcs[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = packedNibbleLookupSubtaskFuture(dsts[taskIdx], srcs[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkPackedNibbleLookup(cpus int, nDstByte int, nJob int, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	srcSlices := make([][]byte, cpus)
	dstSlices := make([][]byte, cpus)
	nSrcByte := (nDstByte + 1) / 2
	for ii := range srcSlices {
		// Add 63 to prevent false sharing.
		newArr := simd.MakeUnsafe(nSrcByte + 63)
		for jj := 0; jj < nSrcByte; jj++ {
			newArr[jj] = byte(jj * 3)
		}
		srcSlices[ii] = newArr[:nSrcByte]
		newArr = simd.MakeUnsafe(nDstByte + 63)
		dstSlices[ii] = newArr[:nDstByte]
	}
	for i := 0; i < b.N; i++ {
		multiPackedNibbleLookup(dstSlices, srcSlices, cpus, nJob)
	}
}

func Benchmark_PackedNibbleLookupShort1(b *testing.B) {
	benchmarkPackedNibbleLookup(1, 75, 9999999, b)
}

func Benchmark_PackedNibbleLookupShort4(b *testing.B) {
	benchmarkPackedNibbleLookup(4, 75, 9999999, b)
}

func Benchmark_PackedNibbleLookupShortMax(b *testing.B) {
	benchmarkPackedNibbleLookup(runtime.NumCPU(), 75, 9999999, b)
}

func Benchmark_PackedNibbleLookupLong1(b *testing.B) {
	benchmarkPackedNibbleLookup(1, 249250621, 50, b)
}

func Benchmark_PackedNibbleLookupLong4(b *testing.B) {
	benchmarkPackedNibbleLookup(4, 249250621, 50, b)
}

func Benchmark_PackedNibbleLookupLongMax(b *testing.B) {
	benchmarkPackedNibbleLookup(runtime.NumCPU(), 249250621, 50, b)
}

func packedNibbleLookupSlow(dst, src []byte, tablePtr *[16]byte) {
	dstLen := len(dst)
	nSrcFullByte := dstLen / 2
	srcOdd := dstLen & 1
	for srcPos := 0; srcPos < nSrcFullByte; srcPos++ {
		srcByte := src[srcPos]
		dst[2*srcPos] = tablePtr[srcByte&15]
		dst[2*srcPos+1] = tablePtr[srcByte>>4]
	}
	if srcOdd == 1 {
		srcByte := src[nSrcFullByte]
		dst[2*nSrcFullByte] = tablePtr[srcByte&15]
	}
}

func TestPackedNibbleLookup(t *testing.T) {
	maxDstSize := 500
	maxSrcSize := (maxDstSize + 1) / 2
	nIter := 200
	srcArr := simd.MakeUnsafe(maxSrcSize)
	dst1Arr := simd.MakeUnsafe(maxDstSize)
	dst2Arr := simd.MakeUnsafe(maxDstSize)
	table := [...]byte{0, 1, 0, 2, 8, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0}
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

func interleaveSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.Interleave8Unsafe(dst, src, src)
	}
	return int(dst[0])
}

func interleaveSubtaskFuture(dst, src []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- interleaveSubtask(dst, src, nIter) }()
	return future
}

func multiInterleave(dsts, srcs [][]byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = interleaveSubtaskFuture(dsts[taskIdx], srcs[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = interleaveSubtaskFuture(dsts[taskIdx], srcs[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkInterleave(cpus int, nSrcByte int, nJob int, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	srcSlices := make([][]byte, cpus)
	dstSlices := make([][]byte, cpus)
	nDstByte := nSrcByte * 2
	for ii := range srcSlices {
		// Add 63 to prevent false sharing.
		newArr := simd.MakeUnsafe(nSrcByte + 63)
		for jj := 0; jj < nSrcByte; jj++ {
			newArr[jj] = byte(jj * 3)
		}
		srcSlices[ii] = newArr[:nSrcByte]
		newArr = simd.MakeUnsafe(nDstByte + 63)
		dstSlices[ii] = newArr[:nDstByte]
	}
	for i := 0; i < b.N; i++ {
		multiInterleave(dstSlices, srcSlices, cpus, nJob)
	}
}

func Benchmark_InterleaveShort1(b *testing.B) {
	benchmarkInterleave(1, 75, 9999999, b)
}

func Benchmark_InterleaveShort4(b *testing.B) {
	benchmarkInterleave(4, 75, 9999999, b)
}

func Benchmark_InterleaveShortMax(b *testing.B) {
	benchmarkInterleave(runtime.NumCPU(), 75, 9999999, b)
}

func Benchmark_InterleaveLong1(b *testing.B) {
	benchmarkInterleave(1, 124625311, 50, b)
}

func Benchmark_InterleaveLong4(b *testing.B) {
	benchmarkInterleave(4, 124625311, 50, b)
}

func Benchmark_InterleaveLongMax(b *testing.B) {
	benchmarkInterleave(runtime.NumCPU(), 124625311, 50, b)
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

func reverse8Subtask(main []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.Reverse8Inplace(main)
	}
	return int(main[0])
}

func reverse8SubtaskFuture(main []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- reverse8Subtask(main, nIter) }()
	return future
}

func multiReverse8(mains [][]byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = reverse8SubtaskFuture(mains[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = reverse8SubtaskFuture(mains[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkReverse8(cpus int, nByte int, nJob int, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	mainSlices := make([][]byte, cpus)
	for ii := range mainSlices {
		// Add 63 to prevent false sharing.
		newArr := simd.MakeUnsafe(nByte + 63)
		for jj := 0; jj < nByte; jj++ {
			newArr[jj] = byte(jj * 3)
		}
		mainSlices[ii] = newArr[:nByte]
	}
	for i := 0; i < b.N; i++ {
		multiReverse8(mainSlices, cpus, nJob)
	}
}

func Benchmark_Reverse8Short1(b *testing.B) {
	benchmarkReverse8(1, 75, 9999999, b)
}

func Benchmark_Reverse8Short4(b *testing.B) {
	benchmarkReverse8(4, 75, 9999999, b)
}

func Benchmark_Reverse8ShortMax(b *testing.B) {
	benchmarkReverse8(runtime.NumCPU(), 75, 9999999, b)
}

func Benchmark_Reverse8Long1(b *testing.B) {
	benchmarkReverse8(1, 249250621, 50, b)
}

func Benchmark_Reverse8Long4(b *testing.B) {
	benchmarkReverse8(4, 249250621, 50, b)
}

func Benchmark_Reverse8LongMax(b *testing.B) {
	benchmarkReverse8(runtime.NumCPU(), 249250621, 50, b)
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
