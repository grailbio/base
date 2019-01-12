// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build !appengine

package simd_test

import (
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"unsafe"

	"github.com/grailbio/base/simd"
)

/*
Initial benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_Memset16Short1-8            20          79792679 ns/op
Benchmark_Memset16Short4-8           100          21460685 ns/op
Benchmark_Memset16ShortMax-8         100          19242532 ns/op
Benchmark_Memset16Long1-8              1        1209730588 ns/op
Benchmark_Memset16Long4-8              1        1630931319 ns/op
Benchmark_Memset16LongMax-8            1        2098725129 ns/op

Benchmark_Reverse16Short1-8                   20          86850717 ns/op
Benchmark_Reverse16Short4-8                   50          26629273 ns/op
Benchmark_Reverse16ShortMax-8                100          21015725 ns/op
Benchmark_Reverse16Long1-8                     1        1241551853 ns/op
Benchmark_Reverse16Long4-8                     1        1691636166 ns/op
Benchmark_Reverse16LongMax-8                   1        2201613448 ns/op

For comparison, memset16:
Benchmark_Memset16Short1-8             5         254778732 ns/op
Benchmark_Memset16Short4-8            20          68925278 ns/op
Benchmark_Memset16ShortMax-8          20          60629923 ns/op
Benchmark_Memset16Long1-8              1        1261998317 ns/op
Benchmark_Memset16Long4-8              1        1684414682 ns/op
Benchmark_Memset16LongMax-8            1        2203954500 ns/op

reverseU16Slow:
Benchmark_Reverse16Short1-8                   10         180262413 ns/op
Benchmark_Reverse16Short4-8                   30          49862651 ns/op
Benchmark_Reverse16ShortMax-8                 10         114370495 ns/op
Benchmark_Reverse16Long1-8                     1        3367505528 ns/op
Benchmark_Reverse16Long4-8                     1        1707333366 ns/op
Benchmark_Reverse16LongMax-8                   1        2175367071 ns/op
*/

func memset16Subtask(dst []uint16, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.RepeatU16(dst, 0x201)
	}
	return int(dst[0])
}

func memset16SubtaskFuture(dst []uint16, nIter int) chan int {
	future := make(chan int)
	go func() { future <- memset16Subtask(dst, nIter) }()
	return future
}

func multiMemset16(dsts [][]uint16, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = memset16SubtaskFuture(dsts[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = memset16SubtaskFuture(dsts[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkMemset16(cpus int, nByte int, nJob int, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	nU16 := (nByte + 1) >> 1
	mainSlices := make([][]uint16, cpus)
	for ii := range mainSlices {
		// Add 31 to prevent false sharing.
		newArr := make([]uint16, nU16, nU16+31)
		for jj := 0; jj < nU16; jj++ {
			newArr[jj] = uint16(jj * 3)
		}
		mainSlices[ii] = newArr[:nU16]
	}
	for i := 0; i < b.N; i++ {
		multiMemset16(mainSlices, cpus, nJob)
	}
}

// Base sequence in length-150 .bam read occupies 75 bytes, so 75 is a good
// size for the short-array benchmark.
func Benchmark_Memset16Short1(b *testing.B) {
	benchmarkMemset16(1, 75, 9999999, b)
}

func Benchmark_Memset16Short4(b *testing.B) {
	benchmarkMemset16(4, 75, 9999999, b)
}

func Benchmark_Memset16ShortMax(b *testing.B) {
	benchmarkMemset16(runtime.NumCPU(), 75, 9999999, b)
}

// GRCh37 chromosome 1 length is 249250621, so that's a plausible long-array
// use case.
func Benchmark_Memset16Long1(b *testing.B) {
	benchmarkMemset16(1, 249250621, 50, b)
}

func Benchmark_Memset16Long4(b *testing.B) {
	benchmarkMemset16(4, 249250621, 50, b)
}

func Benchmark_Memset16LongMax(b *testing.B) {
	benchmarkMemset16(runtime.NumCPU(), 249250621, 50, b)
}

// The compiler clearly recognizes this; performance is almost
// indistinguishable from handcoded assembly.
func memset32Builtin(dst []uint32, val uint32) {
	for idx := range dst {
		dst[idx] = val
	}
}

func TestMemset32(t *testing.T) {
	maxSize := 500
	nIter := 200
	main1Arr := make([]uint32, maxSize)
	main2Arr := make([]uint32, maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		u32Val := rand.Uint32()
		main1Slice := main1Arr[sliceStart:sliceEnd]
		main2Slice := main2Arr[sliceStart:sliceEnd]
		sentinel := rand.Uint32()
		main2Arr[sliceEnd] = sentinel
		memset32Builtin(main1Slice, u32Val)
		main2SliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&main2Slice))
		simd.Memset32Raw(unsafe.Pointer(main2SliceHeader.Data), unsafe.Pointer(&u32Val), main2SliceHeader.Len)
		if !reflect.DeepEqual(main1Slice, main2Slice) {
			t.Fatal("Mismatched Memset32Raw result.")
		}
		if main2Arr[sliceEnd] != sentinel {
			t.Fatal("Memset32Raw clobbered an extra byte.")
		}
	}
}

func memset16(dst []uint16, val uint16) {
	// This tends to be better than the range-for loop, though it's less
	// clear-cut than the memset case.
	nDst := len(dst)
	if nDst != 0 {
		dst[0] = val
		for i := 1; i < nDst; {
			i += copy(dst[i:], dst[:i])
		}
	}
}

func TestMemset16(t *testing.T) {
	maxSize := 500
	nIter := 200
	main1Arr := make([]uint16, maxSize)
	main2Arr := make([]uint16, maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		u16Val := uint16(rand.Uint32())
		main1Slice := main1Arr[sliceStart:sliceEnd]
		main2Slice := main2Arr[sliceStart:sliceEnd]
		sentinel := uint16(rand.Uint32())
		main2Arr[sliceEnd] = sentinel
		memset16(main1Slice, u16Val)
		simd.RepeatU16(main2Slice, u16Val)
		if !reflect.DeepEqual(main1Slice, main2Slice) {
			t.Fatal("Mismatched RepeatU16 result.")
		}
		if main2Arr[sliceEnd] != sentinel {
			t.Fatal("RepeatU16 clobbered an extra byte.")
		}
	}
}

func reverse16Subtask(main []uint16, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.ReverseU16Inplace(main)
	}
	return int(main[0])
}

func reverse16SubtaskFuture(main []uint16, nIter int) chan int {
	future := make(chan int)
	go func() { future <- reverse16Subtask(main, nIter) }()
	return future
}

func multiReverse16(mains [][]uint16, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = reverse16SubtaskFuture(mains[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = reverse16SubtaskFuture(mains[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkReverse16(cpus int, nByte int, nJob int, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	nU16 := (nByte + 1) >> 1
	mainSlices := make([][]uint16, cpus)
	for ii := range mainSlices {
		// Add 31 to prevent false sharing.
		newArr := make([]uint16, nU16, nU16+31)
		for jj := 0; jj < nU16; jj++ {
			newArr[jj] = uint16(jj * 3)
		}
		mainSlices[ii] = newArr[:nU16]
	}
	for i := 0; i < b.N; i++ {
		multiReverse16(mainSlices, cpus, nJob)
	}
}

func Benchmark_Reverse16Short1(b *testing.B) {
	benchmarkReverse16(1, 75, 9999999, b)
}

func Benchmark_Reverse16Short4(b *testing.B) {
	benchmarkReverse16(4, 75, 9999999, b)
}

func Benchmark_Reverse16ShortMax(b *testing.B) {
	benchmarkReverse16(runtime.NumCPU(), 75, 9999999, b)
}

func Benchmark_Reverse16Long1(b *testing.B) {
	benchmarkReverse16(1, 249250621, 50, b)
}

func Benchmark_Reverse16Long4(b *testing.B) {
	benchmarkReverse16(4, 249250621, 50, b)
}

func Benchmark_Reverse16LongMax(b *testing.B) {
	benchmarkReverse16(runtime.NumCPU(), 249250621, 50, b)
}

func reverseU16Slow(main []uint16) {
	nU16 := len(main)
	nU16Div2 := nU16 >> 1
	for idx, invIdx := 0, nU16-1; idx != nU16Div2; idx, invIdx = idx+1, invIdx-1 {
		main[idx], main[invIdx] = main[invIdx], main[idx]
	}
}

func TestReverse16(t *testing.T) {
	maxSize := 500
	nIter := 200
	main1Arr := make([]uint16, maxSize)
	main2Arr := make([]uint16, maxSize)
	main3Arr := make([]uint16, maxSize)
	src2Arr := make([]uint16, maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		main1Slice := main1Arr[sliceStart:sliceEnd]
		main2Slice := main2Arr[sliceStart:sliceEnd]
		main3Slice := main3Arr[sliceStart:sliceEnd]
		src2Slice := src2Arr[sliceStart:sliceEnd]
		for ii := range main1Slice {
			main1Slice[ii] = uint16(rand.Uint32())
		}
		copy(main2Slice, main1Slice)
		copy(src2Slice, main1Slice)
		sentinel := uint16(rand.Uint32())
		main2Arr[sliceEnd] = sentinel
		main3Arr[sliceEnd] = sentinel
		simd.ReverseU16(main3Slice, main1Slice)
		reverseU16Slow(main1Slice)
		simd.ReverseU16Inplace(main2Slice)
		if !reflect.DeepEqual(main1Slice, main2Slice) {
			t.Fatal("Mismatched ReverseU16Inplace result.")
		}
		if main2Arr[sliceEnd] != sentinel {
			t.Fatal("ReverseU16Inplace clobbered an extra byte.")
		}
		if !reflect.DeepEqual(main1Slice, main3Slice) {
			t.Fatal("Mismatched ReverseU16 result.")
		}
		if main3Arr[sliceEnd] != sentinel {
			t.Fatal("ReverseU16 clobbered an extra byte.")
		}
		simd.ReverseU16Inplace(main2Slice)
		if !reflect.DeepEqual(src2Slice, main2Slice) {
			t.Fatal("ReverseU16Inplace didn't invert itself.")
		}
	}
}
