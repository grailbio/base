// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build !appengine

package simd_test

import (
	"math/rand"
	"reflect"
	"testing"
	"unsafe"

	"github.com/grailbio/base/simd"
	"github.com/grailbio/testutil/expect"
)

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
	rand.Seed(1)
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

func memset16Standard(dst []uint16, val uint16) {
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
	rand.Seed(1)
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
		memset16Standard(main1Slice, u16Val)
		simd.RepeatU16(main2Slice, u16Val)
		if !reflect.DeepEqual(main1Slice, main2Slice) {
			t.Fatal("Mismatched RepeatU16 result.")
		}
		if main2Arr[sliceEnd] != sentinel {
			t.Fatal("RepeatU16 clobbered an extra byte.")
		}
	}
}

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_Memset16/SIMDShort1Cpu-8                    10         140130606 ns/op
Benchmark_Memset16/SIMDShortHalfCpu-8                 50          37087600 ns/op
Benchmark_Memset16/SIMDShortAllCpu-8                  50          35361817 ns/op
Benchmark_Memset16/SIMDLong1Cpu-8                      1        1157494604 ns/op
Benchmark_Memset16/SIMDLongHalfCpu-8                   2         921843584 ns/op
Benchmark_Memset16/SIMDLongAllCpu-8                    2         960652822 ns/op
Benchmark_Memset16/StandardShort1Cpu-8                 5         343877390 ns/op
Benchmark_Memset16/StandardShortHalfCpu-8             20          88295789 ns/op
Benchmark_Memset16/StandardShortAllCpu-8              20          86026817 ns/op
Benchmark_Memset16/StandardLong1Cpu-8                  1        1038072481 ns/op
Benchmark_Memset16/StandardLongHalfCpu-8               2         979292703 ns/op
Benchmark_Memset16/StandardLongAllCpu-8                1        1052316741 ns/op
*/

type u16Args struct {
	main []uint16
}

func memset16SimdSubtask(args interface{}, nIter int) int {
	a := args.(u16Args)
	for iter := 0; iter < nIter; iter++ {
		simd.RepeatU16(a.main, 0x201)
	}
	return int(a.main[0])
}

func memset16StandardSubtask(args interface{}, nIter int) int {
	a := args.(u16Args)
	for iter := 0; iter < nIter; iter++ {
		memset16Standard(a.main, 0x201)
	}
	return int(a.main[0])
}

func Benchmark_Memset16(b *testing.B) {
	funcs := []taggedMultiBenchVarargsFunc{
		{
			f:   memset16SimdSubtask,
			tag: "SIMD",
		},
		{
			f:   memset16StandardSubtask,
			tag: "Standard",
		},
	}
	for _, f := range funcs {
		multiBenchmarkVarargs(f.f, f.tag+"Short", 9999999, func() interface{} {
			return u16Args{
				main: make([]uint16, 75, 75+31),
			}
		}, b)
		multiBenchmarkVarargs(f.f, f.tag+"Long", 50, func() interface{} {
			return u16Args{
				main: make([]uint16, 249250622/2, 249250622/2+31),
			}
		}, b)
	}
}

func indexU16Standard(main []uint16, val uint16) int {
	for i, v := range main {
		if v == val {
			return i
		}
	}
	return -1
}

func TestIndexU16(t *testing.T) {
	// Generate nOuterIter random length-arrLen []uint16s, and perform nInnerIter
	// random searches on each slice.
	arrLen := 50000
	nOuterIter := 5
	nInnerIter := 100
	valLimit := 65536 // maximum uint16 is 65535
	rand.Seed(1)
	mainArr := make([]uint16, arrLen)
	for outerIdx := 0; outerIdx < nOuterIter; outerIdx++ {
		for i := range mainArr {
			mainArr[i] = uint16(rand.Intn(valLimit))
		}
		for innerIdx := 0; innerIdx < nInnerIter; innerIdx++ {
			needle := uint16(rand.Intn(valLimit))
			expected := indexU16Standard(mainArr, needle)
			actual := simd.IndexU16(mainArr, needle)
			expect.EQ(t, expected, actual)
		}
	}
}

const indexU16TestLimit = 100

func indexU16SimdSubtask(args interface{}, nIter int) int {
	a := args.(u16Args)
	sum := 0
	needle := uint16(0)
	for iter := 0; iter < nIter; iter++ {
		sum += simd.IndexU16(a.main, needle)
		needle++
		if needle == indexU16TestLimit {
			needle = 0
		}
	}
	return sum
}

func indexU16StandardSubtask(args interface{}, nIter int) int {
	a := args.(u16Args)
	sum := 0
	needle := uint16(0)
	for iter := 0; iter < nIter; iter++ {
		sum += indexU16Standard(a.main, needle)
		needle++
		if needle == indexU16TestLimit {
			needle = 0
		}
	}
	return sum
}

// Single-threaded performance is ~4x as good in my testing.
func Benchmark_IndexU16(b *testing.B) {
	funcs := []taggedMultiBenchVarargsFunc{
		{
			f:   indexU16SimdSubtask,
			tag: "SIMD",
		},
		{
			f:   indexU16StandardSubtask,
			tag: "Standard",
		},
	}
	for _, f := range funcs {
		multiBenchmarkVarargs(f.f, f.tag+"Long", 50, func() interface{} {
			return u16Args{
				main: make([]uint16, 4000000, 4000000+31),
			}
		}, b)
	}
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
	rand.Seed(1)
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

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_ReverseU16Inplace/SIMDShort1Cpu-8                   20         102899505 ns/op
Benchmark_ReverseU16Inplace/SIMDShortHalfCpu-8                50          32918441 ns/op
Benchmark_ReverseU16Inplace/SIMDShortAllCpu-8                 30          38848510 ns/op
Benchmark_ReverseU16Inplace/SIMDLong1Cpu-8                     1        1116384992 ns/op
Benchmark_ReverseU16Inplace/SIMDLongHalfCpu-8                  2         880730467 ns/op
Benchmark_ReverseU16Inplace/SIMDLongAllCpu-8                   2         943204867 ns/op
Benchmark_ReverseU16Inplace/SlowShort1Cpu-8                    3         443056373 ns/op
Benchmark_ReverseU16Inplace/SlowShortHalfCpu-8                10         117142962 ns/op
Benchmark_ReverseU16Inplace/SlowShortAllCpu-8                 10         159087579 ns/op
Benchmark_ReverseU16Inplace/SlowLong1Cpu-8                     1        3158497662 ns/op
Benchmark_ReverseU16Inplace/SlowLongHalfCpu-8                  2         967619258 ns/op
Benchmark_ReverseU16Inplace/SlowLongAllCpu-8                   2         978231337 ns/op
*/

func reverseU16InplaceSimdSubtask(args interface{}, nIter int) int {
	a := args.(u16Args)
	for iter := 0; iter < nIter; iter++ {
		simd.ReverseU16Inplace(a.main)
	}
	return int(a.main[0])
}

func reverseU16InplaceSlowSubtask(args interface{}, nIter int) int {
	a := args.(u16Args)
	for iter := 0; iter < nIter; iter++ {
		reverseU16Slow(a.main)
	}
	return int(a.main[0])
}

func Benchmark_ReverseU16Inplace(b *testing.B) {
	funcs := []taggedMultiBenchVarargsFunc{
		{
			f:   reverseU16InplaceSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   reverseU16InplaceSlowSubtask,
			tag: "Slow",
		},
	}
	for _, f := range funcs {
		multiBenchmarkVarargs(f.f, f.tag+"Short", 9999999, func() interface{} {
			return u16Args{
				main: make([]uint16, 75, 75+31),
			}
		}, b)
		multiBenchmarkVarargs(f.f, f.tag+"Long", 50, func() interface{} {
			return u16Args{
				main: make([]uint16, 249250622/2, 249250622/2+31),
			}
		}, b)
	}
}
