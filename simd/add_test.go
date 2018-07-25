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

Benchmark_AddConstShort1-8            20          65355577 ns/op
Benchmark_AddConstShort4-8           100          22503200 ns/op
Benchmark_AddConstShortMax-8         100          18402022 ns/op
Benchmark_AddConstLong1-8              1        1314456098 ns/op
Benchmark_AddConstLong4-8              1        1963028701 ns/op
Benchmark_AddConstLongMax-8            1        2640851500 ns/op

For comparison, addConst8Slow:
Benchmark_AddConstShort1-8             3         394073399 ns/op
Benchmark_AddConstShort4-8            10         112302717 ns/op
Benchmark_AddConstShortMax-8          10         101881678 ns/op
Benchmark_AddConstLong1-8              1        5806941582 ns/op
Benchmark_AddConstLong4-8              1        2451731455 ns/op
Benchmark_AddConstLongMax-8            1        3305500509 ns/op
*/

func addConstSubtask(dst []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.AddConst8UnsafeInplace(dst, 33)
	}
	return int(dst[0])
}

func addConstSubtaskFuture(dst []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- addConstSubtask(dst, nIter) }()
	return future
}

func multiAddConst(dsts [][]byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = addConstSubtaskFuture(dsts[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = addConstSubtaskFuture(dsts[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkAddConst(cpus int, nByte int, nJob int, b *testing.B) {
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
		multiAddConst(mainSlices, cpus, nJob)
	}
}

// Base sequence in length-150 .bam read occupies 75 bytes, so 75 is a good
// size for the short-array benchmark.
func Benchmark_AddConstShort1(b *testing.B) {
	benchmarkAddConst(1, 75, 9999999, b)
}

func Benchmark_AddConstShort4(b *testing.B) {
	benchmarkAddConst(4, 75, 9999999, b)
}

func Benchmark_AddConstShortMax(b *testing.B) {
	benchmarkAddConst(runtime.NumCPU(), 75, 9999999, b)
}

// GRCh37 chromosome 1 length is 249250621, so that's a plausible long-array
// use case.
func Benchmark_AddConstLong1(b *testing.B) {
	benchmarkAddConst(1, 249250621, 50, b)
}

func Benchmark_AddConstLong4(b *testing.B) {
	benchmarkAddConst(4, 249250621, 50, b)
}

func Benchmark_AddConstLongMax(b *testing.B) {
	benchmarkAddConst(runtime.NumCPU(), 249250621, 50, b)
}

func addConst8Slow(dst []byte, val byte) {
	// strangely, this takes ~35% less time than the single-parameter for loop on
	// the AddConstLong4 benchmark, though performance is usually
	// indistinguishable
	for idx, dstByte := range dst {
		dst[idx] = dstByte + val
	}
}

func TestAddConst(t *testing.T) {
	maxSize := 500
	nIter := 200
	main1Arr := simd.MakeUnsafe(maxSize)
	main2Arr := simd.MakeUnsafe(maxSize)
	main3Arr := simd.MakeUnsafe(maxSize)
	main4Arr := simd.MakeUnsafe(maxSize)
	main5Arr := simd.MakeUnsafe(maxSize)
	src2Arr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		main1Slice := main1Arr[sliceStart:sliceEnd]
		for ii := range main1Slice {
			main1Slice[ii] = byte(rand.Intn(256))
		}
		main2Slice := main2Arr[sliceStart:sliceEnd]
		main3Slice := main3Arr[sliceStart:sliceEnd]
		main4Slice := main4Arr[sliceStart:sliceEnd]
		main5Slice := main5Arr[sliceStart:sliceEnd]
		src2Slice := src2Arr[sliceStart:sliceEnd]
		copy(main2Slice, main1Slice)
		copy(main3Slice, main1Slice)
		copy(src2Slice, main1Slice)
		byteVal := byte(rand.Intn(256))
		simd.AddConst8Unsafe(main4Slice, main1Slice, byteVal)
		sentinel := byte(rand.Intn(256))
		main3Arr[sliceEnd] = sentinel
		main5Arr[sliceEnd] = sentinel
		simd.AddConst8(main5Slice, main1Slice, byteVal)
		addConst8Slow(main1Slice, byteVal)
		if !bytes.Equal(main1Slice, main4Slice) {
			t.Fatal("Mismatched AddConst8Unsafe result.")
		}
		if !bytes.Equal(main1Slice, main5Slice) {
			t.Fatal("Mismatched AddConst8 result.")
		}
		if main5Arr[sliceEnd] != sentinel {
			t.Fatal("AddConst8 clobbered an extra byte.")
		}
		simd.AddConst8UnsafeInplace(main2Slice, byteVal)
		if !bytes.Equal(main1Slice, main2Slice) {
			t.Fatal("Mismatched AddConst8UnsafeInplace result.")
		}
		simd.AddConst8Inplace(main3Slice, byteVal)
		if !bytes.Equal(main1Slice, main3Slice) {
			t.Fatal("Mismatched AddConst8Inplace result.")
		}
		if main3Arr[sliceEnd] != sentinel {
			t.Fatal("AddConst8Inplace clobbered an extra byte.")
		}
		// Verify inverse property.
		simd.AddConst8Inplace(main3Slice, -byteVal)
		if !bytes.Equal(src2Slice, main3Slice) {
			t.Fatal("AddConst8Inplace(., -byteVal) didn't invert AddConst8Inplace(., byteVal).")
		}
	}
}

func subtractFromConst8Slow(dst []byte, val byte) {
	for idx, dstByte := range dst {
		dst[idx] = val - dstByte
	}
}

func TestSubtractFrom(t *testing.T) {
	maxSize := 500
	nIter := 200
	main1Arr := simd.MakeUnsafe(maxSize)
	main2Arr := simd.MakeUnsafe(maxSize)
	main3Arr := simd.MakeUnsafe(maxSize)
	main4Arr := simd.MakeUnsafe(maxSize)
	main5Arr := simd.MakeUnsafe(maxSize)
	src2Arr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		main1Slice := main1Arr[sliceStart:sliceEnd]
		for ii := range main1Slice {
			main1Slice[ii] = byte(rand.Intn(256))
		}
		main2Slice := main2Arr[sliceStart:sliceEnd]
		main3Slice := main3Arr[sliceStart:sliceEnd]
		main4Slice := main4Arr[sliceStart:sliceEnd]
		main5Slice := main5Arr[sliceStart:sliceEnd]
		src2Slice := src2Arr[sliceStart:sliceEnd]
		copy(main2Slice, main1Slice)
		copy(main3Slice, main1Slice)
		copy(src2Slice, main1Slice)
		byteVal := byte(rand.Intn(256))
		simd.SubtractFromConst8Unsafe(main4Slice, main1Slice, byteVal)
		sentinel := byte(rand.Intn(256))
		main3Arr[sliceEnd] = sentinel
		main5Arr[sliceEnd] = sentinel
		simd.SubtractFromConst8(main5Slice, main1Slice, byteVal)
		subtractFromConst8Slow(main1Slice, byteVal)
		if !bytes.Equal(main1Slice, main4Slice) {
			t.Fatal("Mismatched SubtractFromConst8Unsafe result.")
		}
		if !bytes.Equal(main1Slice, main5Slice) {
			t.Fatal("Mismatched SubtractFromConst8 result.")
		}
		if main5Arr[sliceEnd] != sentinel {
			t.Fatal("SubtractFromConst8 clobbered an extra byte.")
		}
		simd.SubtractFromConst8UnsafeInplace(main2Slice, byteVal)
		if !bytes.Equal(main1Slice, main2Slice) {
			t.Fatal("Mismatched SubtractFromConst8UnsafeInplace result.")
		}
		simd.SubtractFromConst8Inplace(main3Slice, byteVal)
		if !bytes.Equal(main1Slice, main3Slice) {
			t.Fatal("Mismatched SubtractFromConst8Inplace result.")
		}
		if main3Arr[sliceEnd] != sentinel {
			t.Fatal("SubtractFromConst8Inplace clobbered an extra byte.")
		}
		// Verify inverse property.
		simd.SubtractFromConst8Inplace(main3Slice, byteVal)
		if !bytes.Equal(src2Slice, main3Slice) {
			t.Fatal("SubtractFromConst8Inplace(., byteVal) didn't invert itself.")
		}
	}
}
