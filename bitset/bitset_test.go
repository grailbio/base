// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package bitset_test

import (
	"math/bits"
	"math/rand"
	"runtime"
	"testing"

	gbitset "github.com/grailbio/base/bitset"
	"github.com/willf/bitset"
)

/*
Initial benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_NonzeroWordLowDensity1-8               5         318053789 ns/op
Benchmark_NonzeroWordLowDensity4-8              20          92268360 ns/op
Benchmark_NonzeroWordLowDensityMax-8            20          75435109 ns/op
Benchmark_NonzeroWordHighDensity1-8              5         338889681 ns/op
Benchmark_NonzeroWordHighDensity4-8             20          93980434 ns/op
Benchmark_NonzeroWordHighDensityMax-8           20          85158994 ns/op

For comparison, using github.com/willf/bitset.NextSet():
Benchmark_NonzeroWordLowDensity1-8               5         295363742 ns/op
Benchmark_NonzeroWordLowDensity4-8              20          78013901 ns/op
Benchmark_NonzeroWordLowDensityMax-8            20          73992701 ns/op
Benchmark_NonzeroWordHighDensity1-8              2         600711815 ns/op
Benchmark_NonzeroWordHighDensity4-8             10         156621467 ns/op
Benchmark_NonzeroWordHighDensityMax-8           10         109333530 ns/op

github.com/willf/bitset.NextSetMany():
Benchmark_NonzeroWordLowDensity1-8               3         362510428 ns/op
Benchmark_NonzeroWordLowDensity4-8              20          98390731 ns/op
Benchmark_NonzeroWordLowDensityMax-8            20          89888478 ns/op
Benchmark_NonzeroWordHighDensity1-8             10         202346572 ns/op
Benchmark_NonzeroWordHighDensity4-8             20          57818033 ns/op
Benchmark_NonzeroWordHighDensityMax-8           30          49601154 ns/op

Manual inlining:
Benchmark_NonzeroWordLowDensity1-8              20          66941143 ns/op
Benchmark_NonzeroWordLowDensity4-8             100          17791558 ns/op
Benchmark_NonzeroWordLowDensityMax-8           100          17825100 ns/op
Benchmark_NonzeroWordHighDensity1-8             20         101415506 ns/op
Benchmark_NonzeroWordHighDensity4-8             50          27927527 ns/op
Benchmark_NonzeroWordHighDensityMax-8           50          23895500 ns/op
*/

func nonzeroWordSubtask(dst, src []uintptr, nIter int) int {
	tot := 0
	nzwPop := 0
	for _, bitWord := range src {
		if bitWord != 0 {
			nzwPop++
		}
	}
	for iter := 0; iter < nIter; iter++ {
		copy(dst, src)
		for s, i := gbitset.NewNonzeroWordScanner(dst, nzwPop); i != -1; i = s.Next() {
			tot += i
		}
	}
	return tot
}

func willfNextSetSubtask(dst, src []uintptr, nIter int) int {
	nBits := uint(len(src) * gbitset.BitsPerWord)
	bsetSrc := bitset.New(nBits)
	for i := uint(0); i != nBits; i++ {
		if gbitset.Test(src, int(i)) {
			bsetSrc.Set(i)
		}
	}
	bsetDst := bitset.New(nBits)

	tot := uint(0)
	for iter := 0; iter < nIter; iter++ {
		bsetSrc.Copy(bsetDst)
		for i, e := bsetDst.NextSet(0); e; i, e = bsetDst.NextSet(i + 1) {
			tot += i
		}
		bsetDst.ClearAll()
	}
	return int(tot)
}

func willfNextSetManySubtask(dst, src []uintptr, nIter int) int {
	nBits := uint(len(src) * gbitset.BitsPerWord)
	bsetSrc := bitset.New(nBits)
	for i := uint(0); i != nBits; i++ {
		if gbitset.Test(src, int(i)) {
			bsetSrc.Set(i)
		}
	}
	bsetDst := bitset.New(nBits)

	tot := uint(0)
	// tried other buffer sizes, 256 seems to be a sweet spot
	var buffer [256]uint
	for iter := 0; iter < nIter; iter++ {
		bsetSrc.Copy(bsetDst)
		for i, buf := bsetDst.NextSetMany(0, buffer[:]); len(buf) > 0; i, buf = bsetDst.NextSetMany(i+1, buf) {
			for j := range buf {
				tot += buf[j]
			}
		}
		bsetDst.ClearAll()
	}
	return int(tot)
}

func bitsetManualInlineSubtask(dst, src []uintptr, nIter int) int {
	tot := 0
	nzwPop := 0
	for _, bitWord := range src {
		if bitWord != 0 {
			nzwPop++
		}
	}
	for iter := 0; iter < nIter; iter++ {
		copy(dst, src)
		nNonzeroWord := nzwPop
		for i, bitWord := range dst {
			if bitWord != 0 {
				bitIdxOffset := i * gbitset.BitsPerWord
				for {
					tot += bits.TrailingZeros64(uint64(bitWord)) + bitIdxOffset
					bitWord &= bitWord - 1
					if bitWord == 0 {
						break
					}
				}
				dst[i] = 0
			}
			nNonzeroWord--
			if nNonzeroWord == 0 {
				break
			}
		}
	}
	return tot
}

func nonzeroWordSubtaskFuture(dst, src []uintptr, nIter int) chan int {
	future := make(chan int)
	// go func() { future <- nonzeroWordSubtask(dst, src, nIter) }()
	// go func() { future <- willfNextSetSubtask(dst, src, nIter) }()
	// go func() { future <- willfNextSetManySubtask(dst, src, nIter) }()
	go func() { future <- bitsetManualInlineSubtask(dst, src, nIter) }()
	return future
}

func multiNonzeroWord(dsts, srcs [][]uintptr, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = nonzeroWordSubtaskFuture(dsts[taskIdx], srcs[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = nonzeroWordSubtaskFuture(dsts[taskIdx], srcs[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkNonzeroWord(cpus, nWord, spacing, nJob int, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	dstSlices := make([][]uintptr, cpus)
	srcSlices := make([][]uintptr, cpus)
	nBits := nWord * gbitset.BitsPerWord
	for ii := range dstSlices {
		// 7 extra capacity to prevent false sharing.
		newDst := make([]uintptr, nWord, nWord+7)
		newSrc := make([]uintptr, nWord, nWord+7)
		for i := spacing - 1; i < nBits; i += spacing {
			gbitset.Set(newSrc, i)
		}
		dstSlices[ii] = newDst
		srcSlices[ii] = newSrc
	}
	for i := 0; i < b.N; i++ {
		multiNonzeroWord(dstSlices, srcSlices, cpus, nJob)
	}
}

func Benchmark_NonzeroWordLowDensity1(b *testing.B) {
	benchmarkNonzeroWord(1, 16, 369, 9999999, b)
}

func Benchmark_NonzeroWordLowDensity4(b *testing.B) {
	benchmarkNonzeroWord(4, 16, 369, 9999999, b)
}

func Benchmark_NonzeroWordLowDensityMax(b *testing.B) {
	benchmarkNonzeroWord(runtime.NumCPU(), 16, 369, 9999999, b)
}

func Benchmark_NonzeroWordHighDensity1(b *testing.B) {
	benchmarkNonzeroWord(1, 16, 1, 99999, b)
}

func Benchmark_NonzeroWordHighDensity4(b *testing.B) {
	benchmarkNonzeroWord(4, 16, 1, 99999, b)
}

func Benchmark_NonzeroWordHighDensityMax(b *testing.B) {
	benchmarkNonzeroWord(runtime.NumCPU(), 16, 1, 99999, b)
}

func naiveBitScanAdder(dst []uintptr) int {
	nBits := len(dst) * gbitset.BitsPerWord
	tot := 0
	for i := 0; i != nBits; i++ {
		if gbitset.Test(dst, i) {
			tot += i
		}
	}
	return tot
}

func TestNonzeroWord(t *testing.T) {
	maxSize := 500
	nIter := 200
	srcArr := make([]uintptr, maxSize)
	dstArr := make([]uintptr, maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		srcSlice := srcArr[sliceStart:sliceEnd]
		dstSlice := dstArr[sliceStart:sliceEnd]

		for i := range srcSlice {
			srcSlice[i] = uintptr(rand.Uint64())
		}
		copy(dstSlice, srcSlice)
		nzwPop := 0
		for _, bitWord := range dstSlice {
			if bitWord != 0 {
				nzwPop++
			}
		}
		if nzwPop == 0 {
			continue
		}

		tot1 := 0
		for s, i := gbitset.NewNonzeroWordScanner(dstSlice, nzwPop); i != -1; i = s.Next() {
			tot1 += i
		}
		tot2 := naiveBitScanAdder(srcSlice)
		if tot1 != tot2 {
			t.Fatal("Mismatched bit-index sums.")
		}
		for _, bitWord := range dstSlice {
			if bitWord != 0 {
				t.Fatal("NonzeroWordScanner failed to clear all words.")
			}
		}
	}
}
