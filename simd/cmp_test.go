// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package simd_test

import (
	"math/rand"
	"runtime"
	"testing"

	"github.com/grailbio/base/simd"
)

/*
Initial benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_FirstUnequalShort1-8                20          63531902 ns/op
Benchmark_FirstUnequalShort4-8               100          17527367 ns/op
Benchmark_FirstUnequalShortMax-8             100          16960390 ns/op
Benchmark_FirstUnequalLong1-8                  2         730374209 ns/op
Benchmark_FirstUnequalLong4-8                  3         334514352 ns/op
Benchmark_FirstUnequalLongMax-8                5         296666922 ns/op

(bytes.Compare()'s speed is essentially identical.)

Benchmark_FirstLeqShort1-8            20          66917028 ns/op
Benchmark_FirstLeqShort4-8           100          18748334 ns/op
Benchmark_FirstLeqShortMax-8         100          18819918 ns/op
Benchmark_FirstLeqLong1-8              3         402510849 ns/op
Benchmark_FirstLeqLong4-8             10         118810967 ns/op
Benchmark_FirstLeqLongMax-8           10         122304803 ns/op

For reference, firstUnequal8Slow has the following results:
Benchmark_FirstUnequalShort1-8                 5         255419211 ns/op
Benchmark_FirstUnequalShort4-8                20          72590461 ns/op
Benchmark_FirstUnequalShortMax-8              20          68392202 ns/op
Benchmark_FirstUnequalLong1-8                  1        4258976363 ns/op
Benchmark_FirstUnequalLong4-8                  1        1088713962 ns/op
Benchmark_FirstUnequalLongMax-8                1        1326682888 ns/op

firstLeq8Slow:
Benchmark_FirstLeqShort1-8             5         248776883 ns/op
Benchmark_FirstLeqShort4-8            20          67078584 ns/op
Benchmark_FirstLeqShortMax-8          20          65117954 ns/op
Benchmark_FirstLeqLong1-8              1        3972184399 ns/op
Benchmark_FirstLeqLong4-8              1        1069477371 ns/op
Benchmark_FirstLeqLongMax-8            1        1238397122 ns/op
*/

func firstUnequalSubtask(arg1, arg2 []byte, nIter int) int {
	curPos := 0
	endPos := len(arg1)
	for iter := 0; iter < nIter; iter++ {
		if curPos >= endPos {
			curPos = 0
		}
		curPos = simd.FirstUnequal8Unsafe(arg1, arg2, curPos)
		curPos++
	}
	return curPos
}

func firstUnequalSubtaskFuture(arg1, arg2 []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- firstUnequalSubtask(arg1, arg2, nIter) }()
	return future
}

func multiFirstUnequal(arg1s, arg2s [][]byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = firstUnequalSubtaskFuture(arg1s[0], arg2s[0], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = firstUnequalSubtaskFuture(arg1s[0], arg2s[0], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkFirstUnequal(cpus int, nByte int, nJob int, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	arg1Slices := make([][]byte, 1)
	for ii := range arg1Slices {
		// Add 63 to prevent false sharing.
		newArr := simd.MakeUnsafe(nByte + 63)
		arg1Slices[ii] = newArr[:nByte]
	}
	arg2Slices := make([][]byte, 1)
	for ii := range arg2Slices {
		newArr := simd.MakeUnsafe(nByte + 63)
		arg2Slices[ii] = newArr[:nByte]
		arg2Slices[ii][nByte/2] = 128
	}
	for i := 0; i < b.N; i++ {
		multiFirstUnequal(arg1Slices, arg2Slices, cpus, nJob)
	}
}

func Benchmark_FirstUnequalShort1(b *testing.B) {
	benchmarkFirstUnequal(1, 75, 9999999, b)
}

func Benchmark_FirstUnequalShort4(b *testing.B) {
	benchmarkFirstUnequal(4, 75, 9999999, b)
}

func Benchmark_FirstUnequalShortMax(b *testing.B) {
	benchmarkFirstUnequal(runtime.NumCPU(), 75, 9999999, b)
}

func Benchmark_FirstUnequalLong1(b *testing.B) {
	benchmarkFirstUnequal(1, 249250621, 50, b)
}

func Benchmark_FirstUnequalLong4(b *testing.B) {
	benchmarkFirstUnequal(4, 249250621, 50, b)
}

func Benchmark_FirstUnequalLongMax(b *testing.B) {
	benchmarkFirstUnequal(runtime.NumCPU(), 249250621, 50, b)
}

func firstUnequal8Slow(arg1, arg2 []byte, startPos int) int {
	// Slow, but straightforward-to-verify implementation.
	endPos := len(arg1)
	for pos := startPos; pos < endPos; pos++ {
		if arg1[pos] != arg2[pos] {
			return pos
		}
	}
	return endPos
}

func TestFirstUnequal(t *testing.T) {
	// Generate some random pairs of strings with varying frequencies of equal
	// bytes, and verify that iterating through the strings with
	// firstUnequal8Slow generates the same sequences of indexes as
	// simd.FirstUnequal8{Unsafe}.
	maxSize := 500
	nIter := 200
	main1Arr := simd.MakeUnsafe(maxSize)
	main2Arr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		main1Slice := main1Arr[sliceStart:sliceEnd]
		for ii := range main1Slice {
			main1Slice[ii] = byte(rand.Intn(256))
		}
		main2Slice := main2Arr[sliceStart:sliceEnd]
		copy(main2Slice, main1Slice)
		sliceSize := sliceEnd - sliceStart
		nDiff := rand.Intn(sliceSize + 1)
		for ii := 0; ii < nDiff; ii++ {
			// This may choose the same position multiple times; that's ok.  Also ok
			// if the new byte randomly matches what it previously was.
			pos := rand.Intn(sliceSize)
			main2Slice[pos] = byte(rand.Intn(256))
		}
		curPos := sliceStart
		for {
			unsafePos := simd.FirstUnequal8Unsafe(main1Slice, main2Slice, curPos)
			safePos := simd.FirstUnequal8(main1Slice, main2Slice, curPos)
			curPos = firstUnequal8Slow(main1Slice, main2Slice, curPos)
			if curPos != safePos {
				t.Fatal("Mismatched FirstUnequal8 result.")
			}
			if curPos != unsafePos {
				t.Fatal("Mismatched FirstUnequal8Unsafe result.")
			}
			curPos++
			if curPos >= sliceSize {
				break
			}
		}
	}
}

func firstGreater8Slow(arg []byte, val byte, startPos int) int {
	// Slow, but straightforward-to-verify implementation.
	endPos := len(arg)
	for pos := startPos; pos < endPos; pos++ {
		if arg[pos] > val {
			return pos
		}
	}
	return endPos
}

func TestFirstGreater(t *testing.T) {
	// Generate random strings and random int8s to compare against, and verify
	// that iterating through the strings with firstGreater8Slow generates
	// the same sequences of indexes as simd.FirstGreater8{Unsafe}.
	maxSize := 500
	nIter := 200
	mainArr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		mainSlice := mainArr[sliceStart:sliceEnd]
		for ii := range mainSlice {
			mainSlice[ii] = byte(rand.Intn(256))
		}
		cmpVal := byte(rand.Intn(256))
		sliceSize := sliceEnd - sliceStart
		curPos := sliceStart
		for {
			unsafePos := simd.FirstGreater8Unsafe(mainSlice, cmpVal, curPos)
			safePos := simd.FirstGreater8(mainSlice, cmpVal, curPos)
			curPos = firstGreater8Slow(mainSlice, cmpVal, curPos)
			if curPos != safePos {
				t.Fatal("Mismatched FirstGreater8 result.")
			}
			if curPos != unsafePos {
				t.Fatal("Mismatched FirstGreater8Unsafe result.")
			}
			curPos++
			if curPos >= sliceSize {
				break
			}
		}
	}
}

func firstLeqSubtask(arg []byte, nIter int) int {
	curPos := 0
	endPos := len(arg)
	for iter := 0; iter < nIter; iter++ {
		if curPos >= endPos {
			curPos = 0
		}
		curPos = simd.FirstLeq8Unsafe(arg, 0, curPos)
		curPos++
	}
	return curPos
}

func firstLeqSubtaskFuture(arg []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- firstLeqSubtask(arg, nIter) }()
	return future
}

func multiFirstLeq(args [][]byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = firstLeqSubtaskFuture(args[0], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = firstLeqSubtaskFuture(args[0], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkFirstLeq(cpus int, nByte int, nJob int, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	argSlices := make([][]byte, 1)
	for ii := range argSlices {
		// Add 63 to prevent false sharing.
		newArr := simd.MakeUnsafe(nByte + 63)
		simd.Memset8(newArr, 255)
		// Just change one byte in the middle.
		newArr[nByte/2] = 0
		argSlices[ii] = newArr[:nByte]
	}
	for i := 0; i < b.N; i++ {
		multiFirstLeq(argSlices, cpus, nJob)
	}
}

func Benchmark_FirstLeqShort1(b *testing.B) {
	benchmarkFirstLeq(1, 75, 9999999, b)
}

func Benchmark_FirstLeqShort4(b *testing.B) {
	benchmarkFirstLeq(4, 75, 9999999, b)
}

func Benchmark_FirstLeqShortMax(b *testing.B) {
	benchmarkFirstLeq(runtime.NumCPU(), 75, 9999999, b)
}

func Benchmark_FirstLeqLong1(b *testing.B) {
	benchmarkFirstLeq(1, 249250621, 50, b)
}

func Benchmark_FirstLeqLong4(b *testing.B) {
	benchmarkFirstLeq(4, 249250621, 50, b)
}

func Benchmark_FirstLeqLongMax(b *testing.B) {
	benchmarkFirstLeq(runtime.NumCPU(), 249250621, 50, b)
}

func firstLeq8Slow(arg []byte, val byte, startPos int) int {
	// Slow, but straightforward-to-verify implementation.
	endPos := len(arg)
	for pos := startPos; pos < endPos; pos++ {
		if arg[pos] <= val {
			return pos
		}
	}
	return endPos
}

func TestFirstLeq8(t *testing.T) {
	// Generate random strings and random int8s to compare against, and verify
	// that iterating through the strings with firstLeq8Slow generates the
	// same sequences of indexes as simd.FirstLeq8{Unsafe}.
	maxSize := 500
	nIter := 200
	mainArr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		mainSlice := mainArr[sliceStart:sliceEnd]
		for ii := range mainSlice {
			mainSlice[ii] = byte(rand.Intn(256))
		}
		cmpVal := byte(rand.Intn(256))
		sliceSize := sliceEnd - sliceStart
		curPos := sliceStart
		for {
			unsafePos := simd.FirstLeq8Unsafe(mainSlice, cmpVal, curPos)
			safePos := simd.FirstLeq8(mainSlice, cmpVal, curPos)
			curPos = firstLeq8Slow(mainSlice, cmpVal, curPos)
			if curPos != safePos {
				t.Fatal("Mismatched FirstLeq8 result.")
			}
			if curPos != unsafePos {
				t.Fatal("Mismatched FirstLeq8Unsafe result.")
			}
			curPos++
			if curPos >= sliceSize {
				break
			}
		}
	}
}
