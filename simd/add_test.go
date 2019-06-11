// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package simd_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/grailbio/base/simd"
)

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

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_AddConst8Inplace/SIMDShort1Cpu-8                    20          94449590 ns/op
Benchmark_AddConst8Inplace/SIMDShortHalfCpu-8                 50          28197917 ns/op
Benchmark_AddConst8Inplace/SIMDShortAllCpu-8                  50          27452313 ns/op
Benchmark_AddConst8Inplace/SIMDLong1Cpu-8                      1        1145256373 ns/op
Benchmark_AddConst8Inplace/SIMDLongHalfCpu-8                   2         959236835 ns/op
Benchmark_AddConst8Inplace/SIMDLongAllCpu-8                    2         982555560 ns/op
Benchmark_AddConst8Inplace/SlowShort1Cpu-8                     2         707287108 ns/op
Benchmark_AddConst8Inplace/SlowShortHalfCpu-8                 10         199415710 ns/op
Benchmark_AddConst8Inplace/SlowShortAllCpu-8                   5         245220685 ns/op
Benchmark_AddConst8Inplace/SlowLong1Cpu-8                      1        5480013373 ns/op
Benchmark_AddConst8Inplace/SlowLongHalfCpu-8                   1        1467424090 ns/op
Benchmark_AddConst8Inplace/SlowLongAllCpu-8                    1        1554565031 ns/op
*/

func addConst8InplaceSimdSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.AddConst8Inplace(dst, 33)
	}
	return int(dst[0])
}

func addConst8InplaceSlowSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		addConst8Slow(dst, 33)
	}
	return int(dst[0])
}

func Benchmark_AddConst8Inplace(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   addConst8InplaceSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   addConst8InplaceSlowSubtask,
			tag: "Slow",
		},
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Short", 150, 0, 9999999, b)
		// GRCh37 chromosome 1 length is 249250621, so that's a plausible
		// long-array use case.
		multiBenchmark(f.f, f.tag+"Long", 249250621, 0, 50, b)
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
