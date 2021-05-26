// Copyright 2021 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package simd_test

import (
	"math"
	"math/rand"
	"testing"

	"github.com/grailbio/base/simd"
	"github.com/grailbio/testutil/expect"
)

func findNaNOrInf64Standard(data []float64) int {
	for i, x := range data {
		if math.IsNaN(x) || (x > math.MaxFloat64) || (x < -math.MaxFloat64) {
			return i
		}
	}
	return -1
}

func getPossiblyNaNOrInfFloat64(rate float64) float64 {
	var x float64
	if rand.Float64() < rate {
		r := rand.Intn(3)
		if r == 0 {
			x = math.NaN()
		} else {
			// -inf if r == 1, +inf if r == 2.
			x = math.Inf(r - 2)
		}
	} else {
		// Exponentially-distributed random number in
		// [-math.MaxFloat64, math.MaxFloat64].
		x = rand.ExpFloat64()
		if rand.Intn(2) != 0 {
			x = -x
		}
	}
	return x
}

func TestFindNaNOrInf(t *testing.T) {
	// Exhausively test all first-NaN/inf positions for sizes in 0..32.
	for size := 0; size <= 32; size++ {
		slice := make([]float64, size)
		got := simd.FindNaNOrInf64(slice)
		want := findNaNOrInf64Standard(slice)
		expect.EQ(t, got, want)
		expect.EQ(t, got, -1)

		for target := size - 1; target >= 0; target-- {
			slice[target] = math.Inf(1)
			// Randomize everything after this position, maximizing entropy.
			for i := target + 1; i < size; i++ {
				slice[i] = getPossiblyNaNOrInfFloat64(0.5)
			}
			got = simd.FindNaNOrInf64(slice)
			want = findNaNOrInf64Standard(slice)
			expect.EQ(t, got, want)
			expect.EQ(t, got, target)
		}
		for i := range slice {
			slice[i] = 0.0
		}
		for target := size - 1; target >= 0; target-- {
			slice[target] = math.NaN()
			for i := target + 1; i < size; i++ {
				slice[i] = getPossiblyNaNOrInfFloat64(0.5)
			}
			got = simd.FindNaNOrInf64(slice)
			want = findNaNOrInf64Standard(slice)
			expect.EQ(t, got, want)
			expect.EQ(t, got, target)
		}
	}
	// Random test for larger sizes.
	maxSize := 30000
	nIter := 200
	rand.Seed(1)
	for iter := 0; iter < nIter; iter++ {
		size := 1 + rand.Intn(maxSize)
		rate := rand.Float64()
		slice := make([]float64, size)
		for i := range slice {
			slice[i] = getPossiblyNaNOrInfFloat64(rate)
		}

		for pos := 0; ; {
			got := simd.FindNaNOrInf64(slice[pos:])
			want := findNaNOrInf64Standard(slice[pos:])
			expect.EQ(t, got, want)
			if got == -1 {
				break
			}
			pos += got + 1
		}
	}
}

type float64Args struct {
	main []float64
}

func findNaNOrInfSimdSubtask(args interface{}, nIter int) int {
	a := args.(float64Args)
	slice := a.main
	sum := 0
	pos := 0
	for iter := 0; iter < nIter; iter++ {
		got := simd.FindNaNOrInf64(slice[pos:])
		sum += got
		if got == -1 {
			pos = 0
		} else {
			pos += got + 1
		}
	}
	return sum
}

func findNaNOrInf64Bitwise(data []float64) int {
	for i, x := range data {
		// Extract the exponent bits, and check if they're all set: that (and only
		// that) corresponds to NaN/inf.
		// Interestingly, the performance of this idiom degrades significantly,
		// relative to
		//   "math.IsNaN(x) || x > math.MaxFloat64 || x < -math.MaxFloat64",
		// if x is interpreted as a float64 anywhere in this loop.
		if (math.Float64bits(x) & (0x7ff << 52)) == (0x7ff << 52) {
			return i
		}
	}
	return -1
}

func findNaNOrInfBitwiseSubtask(args interface{}, nIter int) int {
	a := args.(float64Args)
	slice := a.main
	sum := 0
	pos := 0
	for iter := 0; iter < nIter; iter++ {
		got := findNaNOrInf64Bitwise(slice[pos:])
		sum += got
		if got == -1 {
			pos = 0
		} else {
			pos += got + 1
		}
	}
	return sum
}

func findNaNOrInfStandardSubtask(args interface{}, nIter int) int {
	a := args.(float64Args)
	slice := a.main
	sum := 0
	pos := 0
	for iter := 0; iter < nIter; iter++ {
		got := findNaNOrInf64Standard(slice[pos:])
		sum += got
		if got == -1 {
			pos = 0
		} else {
			pos += got + 1
		}
	}
	return sum
}

// On an m5.16xlarge:
//   $ bazel run //go/src/github.com/grailbio/base/simd:go_default_test -- -test.bench=FindNaNOrInf
//   ...
// Benchmark_FindNaNOrInf/SIMDLong1Cpu-64                82          14053127 ns/op
// Benchmark_FindNaNOrInf/SIMDLongHalfCpu-64            960           1143599 ns/op
// Benchmark_FindNaNOrInf/SIMDLongAllCpu-64            1143           1018525 ns/op
// Benchmark_FindNaNOrInf/BitwiseLong1Cpu-64              8         126930287 ns/op
// Benchmark_FindNaNOrInf/BitwiseLongHalfCpu-64         253           6668467 ns/op
// Benchmark_FindNaNOrInf/BitwiseLongAllCpu-64          229           4679633 ns/op
// Benchmark_FindNaNOrInf/StandardLong1Cpu-64             7         158318559 ns/op
// Benchmark_FindNaNOrInf/StandardLongHalfCpu-64        190           6223669 ns/op
// Benchmark_FindNaNOrInf/StandardLongAllCpu-64         171           6746008 ns/op
// PASS
func Benchmark_FindNaNOrInf(b *testing.B) {
	funcs := []taggedMultiBenchVarargsFunc{
		{
			f:   findNaNOrInfSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   findNaNOrInfBitwiseSubtask,
			tag: "Bitwise",
		},
		{
			f:   findNaNOrInfStandardSubtask,
			tag: "Standard",
		},
	}
	rand.Seed(1)
	for _, f := range funcs {
		multiBenchmarkVarargs(f.f, f.tag+"Long", 100000, func() interface{} {
			main := make([]float64, 30000)
			// Results were overly influenced by RNG if the number of NaNs/infs in
			// the slice was not controlled.
			for i := 0; i < 30; i++ {
				for {
					pos := rand.Intn(len(main))
					if main[pos] != math.Inf(0) {
						main[pos] = math.Inf(0)
						break
					}
				}
			}
			return float64Args{
				main: main,
			}
		}, b)
	}
}
