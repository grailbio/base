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

Benchmark_AndShort1-8                 20          79001639 ns/op
Benchmark_AndShort4-8                100          22674670 ns/op
Benchmark_AndShortMax-8               50          31081823 ns/op
Benchmark_AndLong1-8                   1        2341570438 ns/op
Benchmark_AndLong4-8                   1        2215749534 ns/op
Benchmark_AndLongMax-8                 1        3206256001 ns/op

Benchmark_XorConstShort1-8            20          70140769 ns/op
Benchmark_XorConstShort4-8           100          19764681 ns/op
Benchmark_XorConstShortMax-8         100          18666329 ns/op
Benchmark_XorConstLong1-8              1        1425834375 ns/op
Benchmark_XorConstLong4-8              1        1986577047 ns/op
Benchmark_XorConstLongMax-8            1        2824665438 ns/op

For reference, andInplaceSlow has the following results:
Benchmark_AndShort1-8                  2         568462797 ns/op
Benchmark_AndShort4-8                 10         150619381 ns/op
Benchmark_AndShortMax-8                5         228610110 ns/op
Benchmark_AndLong1-8                   1        8455390684 ns/op
Benchmark_AndLong4-8                   1        5252196746 ns/op
Benchmark_AndLongMax-8                 1        4038874956 ns/op

xorConst8InplaceSlow:
Benchmark_XorConstShort1-8             2         537646968 ns/op
Benchmark_XorConstShort4-8            10         141203425 ns/op
Benchmark_XorConstShortMax-8          10         135202486 ns/op
Benchmark_XorConstLong1-8              1        7982759770 ns/op
Benchmark_XorConstLong4-8              1        4977037903 ns/op
Benchmark_XorConstLongMax-8            1        3903526748 ns/op
*/

func andSubtask(main, arg []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.AndUnsafeInplace(main, arg)
	}
	return int(main[0])
}

func andSubtaskFuture(main, arg []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- andSubtask(main, arg, nIter) }()
	return future
}

func multiAnd(mains [][]byte, arg []byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = andSubtaskFuture(mains[taskIdx], arg, shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = andSubtaskFuture(mains[taskIdx], arg, shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkAnd(cpus int, nByte int, nJob int, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	argArr := simd.MakeUnsafe(nByte)
	for ii := 0; ii < nByte; ii++ {
		argArr[ii] = byte(ii * 6)
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
		multiAnd(mainSlices, argArr, cpus, nJob)
	}
}

// Base sequence in length-150 .bam read occupies 75 bytes, so 75 is a good
// size for the short-array benchmark.
func Benchmark_AndShort1(b *testing.B) {
	benchmarkAnd(1, 75, 9999999, b)
}

func Benchmark_AndShort4(b *testing.B) {
	benchmarkAnd(4, 75, 9999999, b)
}

func Benchmark_AndShortMax(b *testing.B) {
	benchmarkAnd(runtime.NumCPU(), 75, 9999999, b)
}

// GRCh37 chromosome 1 length is 249250621, so that's a plausible long-array
// use case.
func Benchmark_AndLong1(b *testing.B) {
	benchmarkAnd(1, 249250621, 50, b)
}

func Benchmark_AndLong4(b *testing.B) {
	benchmarkAnd(4, 249250621, 50, b)
}

func Benchmark_AndLongMax(b *testing.B) {
	benchmarkAnd(runtime.NumCPU(), 249250621, 50, b)
}

// Don't bother with separate benchmarks for Or/Xor/Invmask.

func andInplaceSlow(main, arg []byte) {
	// Slow, but straightforward-to-verify implementation.
	for idx := range main {
		main[idx] = main[idx] & arg[idx]
	}
}

func TestAnd(t *testing.T) {
	// Generate some random strings and verify that bitwise-and results are as
	// expected.
	maxSize := 500
	nIter := 200
	argArr := simd.MakeUnsafe(maxSize)
	for ii := range argArr {
		argArr[ii] = byte(rand.Intn(256))
	}
	main1Arr := simd.MakeUnsafe(maxSize)
	main2Arr := simd.MakeUnsafe(maxSize)
	main3Arr := simd.MakeUnsafe(maxSize)
	main4Arr := simd.MakeUnsafe(maxSize)
	main5Arr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		argSlice := argArr[sliceStart:sliceEnd]
		main1Slice := main1Arr[sliceStart:sliceEnd]
		for ii := range main1Slice {
			main1Slice[ii] = byte(rand.Intn(256))
		}
		main2Slice := main2Arr[sliceStart:sliceEnd]
		main3Slice := main3Arr[sliceStart:sliceEnd]
		main4Slice := main4Arr[sliceStart:sliceEnd]
		main5Slice := main5Arr[sliceStart:sliceEnd]
		copy(main3Slice, main1Slice)
		copy(main5Slice, main1Slice)
		andInplaceSlow(main1Slice, argSlice)
		simd.AndUnsafe(main2Slice, argSlice, main3Slice)
		if !bytes.Equal(main1Slice, main2Slice) {
			t.Fatal("Mismatched AndUnsafe result.")
		}
		sentinel := byte(rand.Intn(256))
		main4Arr[sliceEnd] = sentinel
		simd.And(main4Slice, argSlice, main3Slice)
		if !bytes.Equal(main1Slice, main4Slice) {
			t.Fatal("Mismatched And result.")
		}
		if main4Arr[sliceEnd] != sentinel {
			t.Fatal("And clobbered an extra byte.")
		}
		simd.AndUnsafeInplace(main3Slice, argSlice)
		if !bytes.Equal(main1Slice, main3Slice) {
			t.Fatal("Mismatched AndUnsafeInplace result.")
		}
		main5Arr[sliceEnd] = sentinel
		simd.AndInplace(main5Slice, argSlice)
		if !bytes.Equal(main1Slice, main5Slice) {
			t.Fatal("Mismatched AndInplace result.")
		}
		if main5Arr[sliceEnd] != sentinel {
			t.Fatal("AndInplace clobbered an extra byte.")
		}
	}
}

func orInplaceSlow(main, arg []byte) {
	for idx := range main {
		main[idx] = main[idx] | arg[idx]
	}
}

func TestOr(t *testing.T) {
	maxSize := 500
	nIter := 200
	argArr := simd.MakeUnsafe(maxSize)
	for ii := range argArr {
		argArr[ii] = byte(rand.Intn(256))
	}
	main1Arr := simd.MakeUnsafe(maxSize)
	main2Arr := simd.MakeUnsafe(maxSize)
	main3Arr := simd.MakeUnsafe(maxSize)
	main4Arr := simd.MakeUnsafe(maxSize)
	main5Arr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		argSlice := argArr[sliceStart:sliceEnd]
		main1Slice := main1Arr[sliceStart:sliceEnd]
		for ii := range main1Slice {
			main1Slice[ii] = byte(rand.Intn(256))
		}
		main2Slice := main2Arr[sliceStart:sliceEnd]
		main3Slice := main3Arr[sliceStart:sliceEnd]
		main4Slice := main4Arr[sliceStart:sliceEnd]
		main5Slice := main5Arr[sliceStart:sliceEnd]
		copy(main3Slice, main1Slice)
		copy(main5Slice, main1Slice)
		orInplaceSlow(main1Slice, argSlice)
		simd.OrUnsafe(main2Slice, argSlice, main3Slice)
		if !bytes.Equal(main1Slice, main2Slice) {
			t.Fatal("Mismatched OrUnsafe result.")
		}
		sentinel := byte(rand.Intn(256))
		main4Arr[sliceEnd] = sentinel
		simd.Or(main4Slice, argSlice, main3Slice)
		if !bytes.Equal(main1Slice, main4Slice) {
			t.Fatal("Mismatched Or result.")
		}
		if main4Arr[sliceEnd] != sentinel {
			t.Fatal("Or clobbered an extra byte.")
		}
		simd.OrUnsafeInplace(main3Slice, argSlice)
		if !bytes.Equal(main1Slice, main3Slice) {
			t.Fatal("Mismatched OrUnsafeInplace result.")
		}
		main5Arr[sliceEnd] = sentinel
		simd.OrInplace(main5Slice, argSlice)
		if !bytes.Equal(main1Slice, main5Slice) {
			t.Fatal("Mismatched OrInplace result.")
		}
		if main5Arr[sliceEnd] != sentinel {
			t.Fatal("OrInplace clobbered an extra byte.")
		}
	}
}

func xorInplaceSlow(main, arg []byte) {
	for idx := range main {
		main[idx] = main[idx] ^ arg[idx]
	}
}

func TestXor(t *testing.T) {
	maxSize := 500
	nIter := 200
	argArr := simd.MakeUnsafe(maxSize)
	for ii := range argArr {
		argArr[ii] = byte(rand.Intn(256))
	}
	main1Arr := simd.MakeUnsafe(maxSize)
	main2Arr := simd.MakeUnsafe(maxSize)
	main3Arr := simd.MakeUnsafe(maxSize)
	main4Arr := simd.MakeUnsafe(maxSize)
	main5Arr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		argSlice := argArr[sliceStart:sliceEnd]
		main1Slice := main1Arr[sliceStart:sliceEnd]
		for ii := range main1Slice {
			main1Slice[ii] = byte(rand.Intn(256))
		}
		main2Slice := main2Arr[sliceStart:sliceEnd]
		main3Slice := main3Arr[sliceStart:sliceEnd]
		main4Slice := main4Arr[sliceStart:sliceEnd]
		main5Slice := main5Arr[sliceStart:sliceEnd]
		copy(main3Slice, main1Slice)
		copy(main5Slice, main1Slice)
		xorInplaceSlow(main1Slice, argSlice)
		simd.XorUnsafe(main2Slice, argSlice, main3Slice)
		if !bytes.Equal(main1Slice, main2Slice) {
			t.Fatal("Mismatched XorUnsafe result.")
		}
		sentinel := byte(rand.Intn(256))
		main4Arr[sliceEnd] = sentinel
		simd.Xor(main4Slice, argSlice, main3Slice)
		if !bytes.Equal(main1Slice, main4Slice) {
			t.Fatal("Mismatched Xor result.")
		}
		if main4Arr[sliceEnd] != sentinel {
			t.Fatal("Xor clobbered an extra byte.")
		}
		simd.XorUnsafeInplace(main3Slice, argSlice)
		if !bytes.Equal(main1Slice, main3Slice) {
			t.Fatal("Mismatched XorUnsafeInplace result.")
		}
		main5Arr[sliceEnd] = sentinel
		simd.XorInplace(main5Slice, argSlice)
		if !bytes.Equal(main1Slice, main5Slice) {
			t.Fatal("Mismatched XorInplace result.")
		}
		if main5Arr[sliceEnd] != sentinel {
			t.Fatal("XorInplace clobbered an extra byte.")
		}
	}
}

func invmaskInplaceSlow(main, invmask []byte) {
	// Slow, but straightforward-to-verify implementation.
	for idx := range main {
		main[idx] = main[idx] &^ invmask[idx]
	}
}

func TestInvmask(t *testing.T) {
	maxSize := 500
	nIter := 200
	invmaskArr := simd.MakeUnsafe(maxSize)
	for ii := range invmaskArr {
		invmaskArr[ii] = byte(rand.Intn(256))
	}
	main1Arr := simd.MakeUnsafe(maxSize)
	main2Arr := simd.MakeUnsafe(maxSize)
	main3Arr := simd.MakeUnsafe(maxSize)
	main4Arr := simd.MakeUnsafe(maxSize)
	main5Arr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		invmaskSlice := invmaskArr[sliceStart:sliceEnd]
		main1Slice := main1Arr[sliceStart:sliceEnd]
		for ii := range main1Slice {
			main1Slice[ii] = byte(rand.Intn(256))
		}
		main2Slice := main2Arr[sliceStart:sliceEnd]
		main3Slice := main3Arr[sliceStart:sliceEnd]
		main4Slice := main4Arr[sliceStart:sliceEnd]
		main5Slice := main5Arr[sliceStart:sliceEnd]
		copy(main3Slice, main1Slice)
		copy(main5Slice, main1Slice)
		invmaskInplaceSlow(main1Slice, invmaskSlice)
		simd.InvmaskUnsafe(main2Slice, main3Slice, invmaskSlice)
		if !bytes.Equal(main1Slice, main2Slice) {
			t.Fatal("Mismatched InvmaskUnsafe result.")
		}
		sentinel := byte(rand.Intn(256))
		main4Arr[sliceEnd] = sentinel
		simd.Invmask(main4Slice, main3Slice, invmaskSlice)
		if !bytes.Equal(main1Slice, main4Slice) {
			t.Fatal("Mismatched Invmask result.")
		}
		if main4Arr[sliceEnd] != sentinel {
			t.Fatal("Invmask clobbered an extra byte.")
		}
		simd.InvmaskUnsafeInplace(main3Slice, invmaskSlice)
		if !bytes.Equal(main1Slice, main3Slice) {
			t.Fatal("Mismatched InvmaskUnsafeInplace result.")
		}
		main5Arr[sliceEnd] = sentinel
		simd.InvmaskInplace(main5Slice, invmaskSlice)
		if !bytes.Equal(main1Slice, main5Slice) {
			t.Fatal("Mismatched InvmaskInplace result.")
		}
		if main5Arr[sliceEnd] != sentinel {
			t.Fatal("InvmaskInplace clobbered an extra byte.")
		}
	}
}

func andConst8InplaceSlow(main []byte, val byte) {
	for idx, mainByte := range main {
		main[idx] = mainByte & val
	}
}

func TestAndConst8(t *testing.T) {
	maxSize := 500
	nIter := 200
	main1Arr := simd.MakeUnsafe(maxSize)
	main2Arr := simd.MakeUnsafe(maxSize)
	main3Arr := simd.MakeUnsafe(maxSize)
	main4Arr := simd.MakeUnsafe(maxSize)
	main5Arr := simd.MakeUnsafe(maxSize)
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
		andVal := byte(rand.Intn(256))
		sentinel := byte(rand.Intn(256))
		copy(main2Slice, main1Slice)
		copy(main3Slice, main1Slice)
		main2Arr[sliceEnd] = sentinel
		main5Arr[sliceEnd] = sentinel
		simd.AndConst8Unsafe(main4Slice, main1Slice, andVal)
		simd.AndConst8(main5Slice, main1Slice, andVal)
		andConst8InplaceSlow(main1Slice, andVal)
		if !bytes.Equal(main1Slice, main4Slice) {
			t.Fatal("Mismatched AndConst8Unsafe result.")
		}
		if !bytes.Equal(main1Slice, main5Slice) {
			t.Fatal("Mismatched AndConst8 result.")
		}
		if main5Arr[sliceEnd] != sentinel {
			t.Fatal("AndConst8 clobbered an extra byte.")
		}
		simd.AndConst8Inplace(main2Slice, andVal)
		if !bytes.Equal(main1Slice, main2Slice) {
			t.Fatal("Mismatched AndConst8Inplace result.")
		}
		if main2Arr[sliceEnd] != sentinel {
			t.Fatal("AndConst8Inplace clobbered an extra byte.")
		}
		simd.AndConst8UnsafeInplace(main3Slice, andVal)
		if !bytes.Equal(main1Slice, main3Slice) {
			t.Fatal("Mismatched AndConst8UnsafeInplace result.")
		}
	}
}

func orConst8InplaceSlow(main []byte, val byte) {
	for idx, mainByte := range main {
		main[idx] = mainByte | val
	}
}

func TestOrConst8(t *testing.T) {
	maxSize := 500
	nIter := 200
	main1Arr := simd.MakeUnsafe(maxSize)
	main2Arr := simd.MakeUnsafe(maxSize)
	main3Arr := simd.MakeUnsafe(maxSize)
	main4Arr := simd.MakeUnsafe(maxSize)
	main5Arr := simd.MakeUnsafe(maxSize)
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
		orVal := byte(rand.Intn(256))
		sentinel := byte(rand.Intn(256))
		copy(main2Slice, main1Slice)
		copy(main3Slice, main1Slice)
		main2Arr[sliceEnd] = sentinel
		main5Arr[sliceEnd] = sentinel
		simd.OrConst8Unsafe(main4Slice, main1Slice, orVal)
		simd.OrConst8(main5Slice, main1Slice, orVal)
		orConst8InplaceSlow(main1Slice, orVal)
		if !bytes.Equal(main4Slice, main1Slice) {
			t.Fatal("Mismatched OrConst8Unsafe result.")
		}
		if !bytes.Equal(main5Slice, main1Slice) {
			t.Fatal("Mismatched OrConst8 result.")
		}
		if main5Arr[sliceEnd] != sentinel {
			t.Fatal("OrConst8 clobbered an extra byte.")
		}
		simd.OrConst8Inplace(main2Slice, orVal)
		if !bytes.Equal(main1Slice, main2Slice) {
			t.Fatal("Mismatched OrConst8Inplace result.")
		}
		if main2Arr[sliceEnd] != sentinel {
			t.Fatal("OrConst8Inplace clobbered an extra byte.")
		}
		simd.OrConst8UnsafeInplace(main3Slice, orVal)
		if !bytes.Equal(main1Slice, main3Slice) {
			t.Fatal("Mismatched OrConst8UnsafeInplace result.")
		}
	}
}

func xorConstSubtask(main []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.XorConst8UnsafeInplace(main, 3)
	}
	return int(main[0])
}

func xorConstSubtaskFuture(main []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- xorConstSubtask(main, nIter) }()
	return future
}

func multiXorConst(mains [][]byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = xorConstSubtaskFuture(mains[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = xorConstSubtaskFuture(mains[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkXorConst(cpus int, nByte int, nJob int, b *testing.B) {
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
		multiXorConst(mainSlices, cpus, nJob)
	}
}

// Base sequence in length-150 .bam read occupies 75 bytes, so 75 is a good
// size for the short-array benchmark.
func Benchmark_XorConstShort1(b *testing.B) {
	benchmarkXorConst(1, 75, 9999999, b)
}

func Benchmark_XorConstShort4(b *testing.B) {
	benchmarkXorConst(4, 75, 9999999, b)
}

func Benchmark_XorConstShortMax(b *testing.B) {
	benchmarkXorConst(runtime.NumCPU(), 75, 9999999, b)
}

// GRCh37 chromosome 1 length is 249250621, so that's a plausible long-array
// use case.
func Benchmark_XorConstLong1(b *testing.B) {
	benchmarkXorConst(1, 249250621, 50, b)
}

func Benchmark_XorConstLong4(b *testing.B) {
	benchmarkXorConst(4, 249250621, 50, b)
}

func Benchmark_XorConstLongMax(b *testing.B) {
	benchmarkXorConst(runtime.NumCPU(), 249250621, 50, b)
}

func xorConst8InplaceSlow(main []byte, val byte) {
	for idx, mainByte := range main {
		main[idx] = mainByte ^ val
	}
}

func TestXorConst8(t *testing.T) {
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
		xorVal := byte(rand.Intn(256))
		sentinel := byte(rand.Intn(256))
		copy(main2Slice, main1Slice)
		copy(main3Slice, main1Slice)
		copy(src2Slice, main1Slice)
		main2Arr[sliceEnd] = sentinel
		main5Arr[sliceEnd] = sentinel
		simd.XorConst8Unsafe(main4Slice, main1Slice, xorVal)
		simd.XorConst8(main5Slice, main1Slice, xorVal)
		xorConst8InplaceSlow(main1Slice, xorVal)
		if !bytes.Equal(main1Slice, main4Slice) {
			t.Fatal("Mismatched XorConst8Unsafe result.")
		}
		if !bytes.Equal(main1Slice, main5Slice) {
			t.Fatal("Mismatched XorConst8 result.")
		}
		if main5Arr[sliceEnd] != sentinel {
			t.Fatal("XorConst8 clobbered an extra byte.")
		}
		simd.XorConst8Inplace(main2Slice, xorVal)
		if !bytes.Equal(main1Slice, main2Slice) {
			t.Fatal("Mismatched XorConst8Inplace result.")
		}
		if main2Arr[sliceEnd] != sentinel {
			t.Fatal("XorConst8Inplace clobbered an extra byte.")
		}
		simd.XorConst8Inplace(main2Slice, xorVal)
		if !bytes.Equal(main2Slice, src2Slice) {
			t.Fatal("XorConst8Inplace did not invert itself.")
		}
		simd.XorConst8UnsafeInplace(main3Slice, xorVal)
		if !bytes.Equal(main1Slice, main3Slice) {
			t.Fatal("Mismatched XorConst8UnsafeInplace result.")
		}
	}
}
