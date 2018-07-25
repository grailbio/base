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

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_AndInplace/SIMDShort1Cpu-8                  20          91832421 ns/op
Benchmark_AndInplace/SIMDShortHalfCpu-8               50          25323744 ns/op
Benchmark_AndInplace/SIMDShortAllCpu-8               100          23869031 ns/op
Benchmark_AndInplace/SIMDLong1Cpu-8                    1        1715379622 ns/op
Benchmark_AndInplace/SIMDLongHalfCpu-8                 1        1372591170 ns/op
Benchmark_AndInplace/SIMDLongAllCpu-8                  1        1427476449 ns/op
Benchmark_AndInplace/SlowShort1Cpu-8                   2         550667201 ns/op
Benchmark_AndInplace/SlowShortHalfCpu-8               10         145756965 ns/op
Benchmark_AndInplace/SlowShortAllCpu-8                10         135311356 ns/op
Benchmark_AndInplace/SlowLong1Cpu-8                    1        7711233274 ns/op
Benchmark_AndInplace/SlowLongHalfCpu-8                 1        2144409827 ns/op
Benchmark_AndInplace/SlowLongAllCpu-8                  1        2158206158 ns/op
*/

func andSimdSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.AndInplace(dst, src)
	}
	return int(dst[0])
}

func andSlowSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		andInplaceSlow(dst, src)
	}
	return int(dst[0])
}

func Benchmark_AndInplace(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   andSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   andSlowSubtask,
			tag: "Slow",
		},
	}
	for _, f := range funcs {
		// This is relevant to .bam reads in packed form, so 150/2=75 is a good
		// size for the short-array benchmark.
		multiBenchmark(f.f, f.tag+"Short", 75, 75, 9999999, b)
		multiBenchmark(f.f, f.tag+"Long", 249250621, 249250621, 50, b)
	}
}

// Don't bother with separate benchmarks for Or/Xor/Invmask.

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

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_XorConst8Inplace/SIMDShort1Cpu-8                    20          79730366 ns/op
Benchmark_XorConst8Inplace/SIMDShortHalfCpu-8                100          21216542 ns/op
Benchmark_XorConst8Inplace/SIMDShortAllCpu-8                 100          18902385 ns/op
Benchmark_XorConst8Inplace/SIMDLong1Cpu-8                      1        1291770636 ns/op
Benchmark_XorConst8Inplace/SIMDLongHalfCpu-8                   2         958003320 ns/op
Benchmark_XorConst8Inplace/SIMDLongAllCpu-8                    2         967333286 ns/op
Benchmark_XorConst8Inplace/SlowShort1Cpu-8                     3         417781174 ns/op
Benchmark_XorConst8Inplace/SlowShortHalfCpu-8                 10         112255124 ns/op
Benchmark_XorConst8Inplace/SlowShortAllCpu-8                  10         100138643 ns/op
Benchmark_XorConst8Inplace/SlowLong1Cpu-8                      1        5476605564 ns/op
Benchmark_XorConst8Inplace/SlowLongHalfCpu-8                   1        1480923705 ns/op
Benchmark_XorConst8Inplace/SlowLongAllCpu-8                    1        1588216831 ns/op
*/

func xorConst8InplaceSimdSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		simd.XorConst8Inplace(dst, 3)
	}
	return int(dst[0])
}

func xorConst8InplaceSlowSubtask(dst, src []byte, nIter int) int {
	for iter := 0; iter < nIter; iter++ {
		xorConst8InplaceSlow(dst, 3)
	}
	return int(dst[0])
}

func Benchmark_XorConst8Inplace(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   xorConst8InplaceSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   xorConst8InplaceSlowSubtask,
			tag: "Slow",
		},
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Short", 75, 0, 9999999, b)
		multiBenchmark(f.f, f.tag+"Long", 249250621, 0, 50, b)
	}
}
