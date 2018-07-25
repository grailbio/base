// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package simd_test

import (
	"math/rand"
	"testing"

	"github.com/grailbio/base/simd"
)

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

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_FirstUnequal8/UnsafeShort1Cpu-8                     10         104339029 ns/op
Benchmark_FirstUnequal8/UnsafeShortHalfCpu-8                  50          28360826 ns/op
Benchmark_FirstUnequal8/UnsafeShortAllCpu-8                  100          24272646 ns/op
Benchmark_FirstUnequal8/UnsafeLong1Cpu-8                       2         654616638 ns/op
Benchmark_FirstUnequal8/UnsafeLongHalfCpu-8                    3         499705618 ns/op
Benchmark_FirstUnequal8/UnsafeLongAllCpu-8                     3         477807746 ns/op
Benchmark_FirstUnequal8/SIMDShort1Cpu-8                       10         114335599 ns/op
Benchmark_FirstUnequal8/SIMDShortHalfCpu-8                    50          30189426 ns/op
Benchmark_FirstUnequal8/SIMDShortAllCpu-8                     50          26847829 ns/op
Benchmark_FirstUnequal8/SIMDLong1Cpu-8                         2         735662635 ns/op
Benchmark_FirstUnequal8/SIMDLongHalfCpu-8                      3         488191229 ns/op
Benchmark_FirstUnequal8/SIMDLongAllCpu-8                       3         480315740 ns/op
Benchmark_FirstUnequal8/SlowShort1Cpu-8                        2         608618106 ns/op
Benchmark_FirstUnequal8/SlowShortHalfCpu-8                    10         166658947 ns/op
Benchmark_FirstUnequal8/SlowShortAllCpu-8                     10         154372585 ns/op
Benchmark_FirstUnequal8/SlowLong1Cpu-8                         1        3883830889 ns/op
Benchmark_FirstUnequal8/SlowLongHalfCpu-8                      1        1080159614 ns/op
Benchmark_FirstUnequal8/SlowLongAllCpu-8                       1        1046794857 ns/op

Notes: There is practically no speed penalty relative to bytes.Compare().
*/

func firstUnequal8UnsafeSubtask(dst, src []byte, nIter int) int {
	curPos := 0
	endPos := len(dst)
	for iter := 0; iter < nIter; iter++ {
		if curPos >= endPos {
			curPos = 0
		}
		curPos = simd.FirstUnequal8Unsafe(dst, src, curPos)
		curPos++
	}
	return curPos
}

func firstUnequal8SimdSubtask(dst, src []byte, nIter int) int {
	curPos := 0
	endPos := len(dst)
	for iter := 0; iter < nIter; iter++ {
		if curPos >= endPos {
			curPos = 0
		}
		curPos = simd.FirstUnequal8(dst, src, curPos)
		curPos++
	}
	return curPos
}

func firstUnequal8SlowSubtask(dst, src []byte, nIter int) int {
	curPos := 0
	endPos := len(dst)
	for iter := 0; iter < nIter; iter++ {
		if curPos >= endPos {
			curPos = 0
		}
		curPos = firstUnequal8Slow(dst, src, curPos)
		curPos++
	}
	return curPos
}

func Benchmark_FirstUnequal8(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   firstUnequal8UnsafeSubtask,
			tag: "Unsafe",
		},
		{
			f:   firstUnequal8SimdSubtask,
			tag: "SIMD",
		},
		{
			f:   firstUnequal8SlowSubtask,
			tag: "Slow",
		},
	}
	// Necessary to customize the initialization functions; the default setting
	// of src = {0, 3, 6, 9, ...} and dst = {0, 0, 0, 0, ...} results in too many
	// mismatches for a realistic benchmark.
	opts := multiBenchmarkOpts{
		dstInit: func(src []byte) {
			src[len(src)/2] = 128
		},
		srcInit: bytesInit0,
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Short", 150, 150, 9999999, b, opts)
		multiBenchmark(f.f, f.tag+"Long", 249250621, 249250621, 50, b, opts)
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

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_FirstLeq8/SIMDShort1Cpu-8                   20          87235782 ns/op
Benchmark_FirstLeq8/SIMDShortHalfCpu-8                50          23864936 ns/op
Benchmark_FirstLeq8/SIMDShortAllCpu-8                100          21211734 ns/op
Benchmark_FirstLeq8/SIMDLong1Cpu-8                     3         402996726 ns/op
Benchmark_FirstLeq8/SIMDLongHalfCpu-8                  5         245066128 ns/op
Benchmark_FirstLeq8/SIMDLongAllCpu-8                   5         231557103 ns/op
Benchmark_FirstLeq8/SlowShort1Cpu-8                    2         549800977 ns/op
Benchmark_FirstLeq8/SlowShortHalfCpu-8                10         152074140 ns/op
Benchmark_FirstLeq8/SlowShortAllCpu-8                 10         142355855 ns/op
Benchmark_FirstLeq8/SlowLong1Cpu-8                     1        3687059961 ns/op
Benchmark_FirstLeq8/SlowLongHalfCpu-8                  1        1030280464 ns/op
Benchmark_FirstLeq8/SlowLongAllCpu-8                   1        1019364554 ns/op
*/

func firstLeq8SimdSubtask(dst, src []byte, nIter int) int {
	curPos := 0
	endPos := len(src)
	for iter := 0; iter < nIter; iter++ {
		if curPos >= endPos {
			curPos = 0
		}
		curPos = simd.FirstLeq8(src, 0, curPos)
		curPos++
	}
	return curPos
}

func firstLeq8SlowSubtask(dst, src []byte, nIter int) int {
	curPos := 0
	endPos := len(src)
	for iter := 0; iter < nIter; iter++ {
		if curPos >= endPos {
			curPos = 0
		}
		curPos = firstLeq8Slow(src, 0, curPos)
		curPos++
	}
	return curPos
}

func Benchmark_FirstLeq8(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   firstLeq8SimdSubtask,
			tag: "SIMD",
		},
		{
			f:   firstLeq8SlowSubtask,
			tag: "Slow",
		},
	}
	opts := multiBenchmarkOpts{
		srcInit: func(src []byte) {
			simd.Memset8(src, 255)
			// Just change one byte in the middle.
			src[len(src)/2] = 128
		},
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Short", 0, 150, 9999999, b, opts)
		multiBenchmark(f.f, f.tag+"Long", 0, 249250621, 50, b, opts)
	}
}
