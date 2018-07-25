// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package simd_test

import (
	"bytes"
	"math/bits"
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

Benchmark_ByteShortPopcnt1-8                   2         880567116 ns/op
Benchmark_ByteShortPopcnt4-8                   5         249242503 ns/op
Benchmark_ByteShortPopcntMax-8                10         197222506 ns/op
Benchmark_ByteShortOldPopcnt1-8                2         992499107 ns/op
Benchmark_ByteShortOldPopcnt4-8                5         273206944 ns/op
Benchmark_ByteShortOldPopcntMax-8              5         270066225 ns/op
Benchmark_ByteLongPopcnt1-8                    1        1985211630 ns/op
Benchmark_ByteLongPopcnt4-8                    2         511618631 ns/op
Benchmark_ByteLongPopcntMax-8                  3         506767183 ns/op
Benchmark_ByteLongOldPopcnt1-8                 1        2473936090 ns/op
Benchmark_ByteLongOldPopcnt4-8                 2         747990164 ns/op
Benchmark_ByteLongOldPopcntMax-8               2         743366724 ns/op

Only differences between new and old code so far are
  (i) 2x loop-unroll, and
  (ii) per-call "do we have SSE4.2" flag checks.
I did try implementing the 2x loop-unroll in the math/bits code, but it did not
have much of an effect there.


Benchmark_CountCGShort1-8             20          86156256 ns/op
Benchmark_CountCGShort4-8            100          23032100 ns/op
Benchmark_CountCGShortMax-8          100          22272840 ns/op
Benchmark_CountCGLong1-8               1        1025820961 ns/op
Benchmark_CountCGLong4-8               1        1461443453 ns/op
Benchmark_CountCGLongMax-8             1        2180675096 ns/op

Benchmark_Count3BytesShort1-8                 10         105075618 ns/op
Benchmark_Count3BytesShort4-8                 50          29571207 ns/op
Benchmark_Count3BytesShortMax-8               50          27188249 ns/op
Benchmark_Count3BytesLong1-8                   1        1308949833 ns/op
Benchmark_Count3BytesLong4-8                   1        1612130605 ns/op
Benchmark_Count3BytesLongMax-8                 1        2319478110 ns/op

Benchmark_Accumulate8Short1-8                 20          67181978 ns/op
Benchmark_Accumulate8Short4-8                100          18216505 ns/op
Benchmark_Accumulate8ShortMax-8              100          17359664 ns/op
Benchmark_Accumulate8Long1-8                   1        1050107761 ns/op
Benchmark_Accumulate8Long4-8                   1        1440863620 ns/op
Benchmark_Accumulate8LongMax-8                 1        2202725361 ns/op

Benchmark_Accumulate8GreaterShort1-8                  20          91913187 ns/op
Benchmark_Accumulate8GreaterShort4-8                  50          25629176 ns/op
Benchmark_Accumulate8GreaterShortMax-8               100          22020836 ns/op
Benchmark_Accumulate8GreaterLong1-8                    1        1166256065 ns/op
Benchmark_Accumulate8GreaterLong4-8                    1        1529133163 ns/op
Benchmark_Accumulate8GreaterLongMax-8                  1        2447755677 ns/op

For comparison, countCGStandard:
Benchmark_CountCGShort1-8              5         206159939 ns/op
Benchmark_CountCGShort4-8             20          55653414 ns/op
Benchmark_CountCGShortMax-8           30          49566408 ns/op
Benchmark_CountCGLong1-8               1        1786864086 ns/op
Benchmark_CountCGLong4-8               1        1975270955 ns/op
Benchmark_CountCGLongMax-8             1        2846417721 ns/op

countCGNaive:
Benchmark_CountCGShort1-8              2         753564012 ns/op
Benchmark_CountCGShort4-8              5         200074546 ns/op
Benchmark_CountCGShortMax-8           10         193392413 ns/op
Benchmark_CountCGLong1-8               1        12838546141 ns/op
Benchmark_CountCGLong4-8               1        4371080727 ns/op
Benchmark_CountCGLongMax-8             1        5023199989 ns/op
(lesson: don't forget to use bytes.Count() when it's applicable!)

count3BytesStandard:
Benchmark_Count3BytesShort1-8                  5         288822460 ns/op
Benchmark_Count3BytesShort4-8                 20          81116028 ns/op
Benchmark_Count3BytesShortMax-8               20          75587001 ns/op
Benchmark_Count3BytesLong1-8                   1        2526123231 ns/op
Benchmark_Count3BytesLong4-8                   1        2425857828 ns/op
Benchmark_Count3BytesLongMax-8                 1        3235725694 ns/op

accumulate8Slow:
Benchmark_Accumulate8Short1-8                  3         394838027 ns/op
Benchmark_Accumulate8Short4-8                 10         105763035 ns/op
Benchmark_Accumulate8ShortMax-8               20          93473300 ns/op
Benchmark_Accumulate8Long1-8                   1        5143881564 ns/op
Benchmark_Accumulate8Long4-8                   1        3501219437 ns/op
Benchmark_Accumulate8LongMax-8                 1        3096559063 ns/op

accumulate8GreaterSlow:
Benchmark_Accumulate8GreaterShort1-8                   3         466978266 ns/op
Benchmark_Accumulate8GreaterShort4-8                  10         125637387 ns/op
Benchmark_Accumulate8GreaterShortMax-8                10         117808985 ns/op
Benchmark_Accumulate8GreaterLong1-8                    1        9825147670 ns/op
Benchmark_Accumulate8GreaterLong4-8                    1        5815093074 ns/op
Benchmark_Accumulate8GreaterLongMax-8                  1        4554119137 ns/op
*/

func init() {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		// popcnt_amd64.go shouldn't compile at all in this case, but just in
		// case...
		panic("8-byte words required.")
	}
}

func popcntBytesNoasm(byteslice []byte) int {
	bytesliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&byteslice))
	ct := uintptr(len(byteslice))

	bytearr := unsafe.Pointer(bytesliceHeader.Data)
	endptr := unsafe.Pointer(uintptr(bytearr) + ct)
	tot := 0
	nLeadingByte := ct % 8
	if nLeadingByte != 0 {
		leadingWord := uint64(0)
		if (nLeadingByte & 1) != 0 {
			leadingWord = (uint64)(*(*byte)(bytearr))
			bytearr = unsafe.Pointer(uintptr(bytearr) + 1)
		}
		if (nLeadingByte & 2) != 0 {
			leadingWord <<= 16
			leadingWord |= (uint64)(*(*uint16)(bytearr))
			bytearr = unsafe.Pointer(uintptr(bytearr) + 2)
		}
		if (nLeadingByte & 4) != 0 {
			leadingWord <<= 32
			leadingWord |= (uint64)(*(*uint32)(bytearr))
			bytearr = unsafe.Pointer(uintptr(bytearr) + 4)
		}
		tot = bits.OnesCount64(leadingWord)
	}
	// Strangely, performance of this loop seems to vary by ~20% on my Mac,
	// depending on which of several equivalent ways I use to write it.
	for bytearr != endptr {
		tot += bits.OnesCount64((uint64)(*((*uint64)(bytearr))))
		bytearr = unsafe.Pointer(uintptr(bytearr) + 8)
	}
	return tot
}

func popcntSubtaskOld(byteSlice []byte, nIter int) int {
	sum := 0
	for iter := 0; iter < nIter; iter++ {
		sum += popcntBytesNoasm(byteSlice)
	}
	return sum
}

func popcntSubtaskOldFuture(byteSlice []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- popcntSubtaskOld(byteSlice, nIter) }()
	return future
}

func popcntSubtask(byteSlice []byte, nIter int) int {
	sum := 0
	for iter := 0; iter < nIter; iter++ {
		sum += simd.Popcnt(byteSlice)
	}
	return sum
}

func popcntSubtaskFuture(byteSlice []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- popcntSubtask(byteSlice, nIter) }()
	return future
}

func multiBytePopcnt(byteSlice []byte, cpus int, nJob int, useOld bool) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	// Note that this straightforward sharding scheme sometimes doesn't work well
	// with hyperthreading: on my Mac, I get more consistent performance dividing
	// cpus by two to set it to the actual number of cores.  However, this
	// doesn't happen on my adhoc instance.
	// In any case, I'll experiment with other concurrency patterns soon.
	var taskIdx int
	if useOld {
		for ; taskIdx < shardRemainder; taskIdx++ {
			sumFutures[taskIdx] = popcntSubtaskOldFuture(byteSlice, shardSizeP1)
		}
		for ; taskIdx < cpus; taskIdx++ {
			sumFutures[taskIdx] = popcntSubtaskOldFuture(byteSlice, shardSizeBase)
		}
	} else {
		for ; taskIdx < shardRemainder; taskIdx++ {
			sumFutures[taskIdx] = popcntSubtaskFuture(byteSlice, shardSizeP1)
		}
		for ; taskIdx < cpus; taskIdx++ {
			sumFutures[taskIdx] = popcntSubtaskFuture(byteSlice, shardSizeBase)
		}
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
	// fmt.Println(sum)
}

func benchmarkBytePopcnt(cpus int, nByte int, nJob int, useOld bool, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	byteArr := make([]byte, nByte+1)
	for uii := uint(0); uii < uint(nByte); uii++ {
		byteArr[uii] = (byte)(uii)
	}
	byteSlice := byteArr[1 : nByte+1] // force unaligned
	for i := 0; i < b.N; i++ {
		multiBytePopcnt(byteSlice, cpus, nJob, useOld)
	}
}

// Base sequence in length-150 .bam read occupies 75 bytes, so 75 is a good
// size for the short-array benchmark.
func Benchmark_ByteShortPopcnt1(b *testing.B) {
	benchmarkBytePopcnt(1, 75, 99999999, false, b)
}

func Benchmark_ByteShortPopcnt4(b *testing.B) {
	benchmarkBytePopcnt(4, 75, 99999999, false, b)
}

func Benchmark_ByteShortPopcntMax(b *testing.B) {
	benchmarkBytePopcnt(runtime.NumCPU(), 75, 99999999, false, b)
}

func Benchmark_ByteShortOldPopcnt1(b *testing.B) {
	benchmarkBytePopcnt(1, 75, 99999999, true, b)
}

func Benchmark_ByteShortOldPopcnt4(b *testing.B) {
	benchmarkBytePopcnt(4, 75, 99999999, true, b)
}

func Benchmark_ByteShortOldPopcntMax(b *testing.B) {
	benchmarkBytePopcnt(runtime.NumCPU(), 75, 99999999, true, b)
}

// GRCh37 chromosome 1 length is 249250621, so that's a plausible long-array
// use case.
func Benchmark_ByteLongPopcnt1(b *testing.B) {
	benchmarkBytePopcnt(1, 249250621, 100, false, b)
}

func Benchmark_ByteLongPopcnt4(b *testing.B) {
	benchmarkBytePopcnt(4, 249250621, 100, false, b)
}

func Benchmark_ByteLongPopcntMax(b *testing.B) {
	benchmarkBytePopcnt(runtime.NumCPU(), 249250621, 100, false, b)
}

func Benchmark_ByteLongOldPopcnt1(b *testing.B) {
	benchmarkBytePopcnt(1, 249250621, 100, true, b)
}

func Benchmark_ByteLongOldPopcnt4(b *testing.B) {
	benchmarkBytePopcnt(4, 249250621, 100, true, b)
}

func Benchmark_ByteLongOldPopcntMax(b *testing.B) {
	benchmarkBytePopcnt(runtime.NumCPU(), 249250621, 100, true, b)
}

func popcntBytesSlow(bytes []byte) int {
	// Slow (factor of 5-8x), but straightforward-to-verify implementation.
	tot := 0
	for _, b := range bytes {
		tot += bits.OnesCount8(b)
	}
	return tot
}

func TestBytePopcnt(t *testing.T) {
	// Generate a random string, then popcount 20000 random slices with lengths
	// in [0, 5000).
	maxSize := 5000
	nIter := 20000
	byteArr := make([]byte, 2*maxSize)
	for i := range byteArr {
		byteArr[i] = byte(rand.Intn(256))
	}
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize)
		curSlice := byteArr[sliceStart:sliceEnd]
		sum1 := simd.Popcnt(curSlice)
		sum2 := popcntBytesNoasm(curSlice)
		if sum1 != sum2 {
			t.Fatal("Mismatched popcounts (noasm).")
		}
	}
	nVerifyIter := 1000
	for iter := 0; iter < nVerifyIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize)
		curSlice := byteArr[sliceStart:sliceEnd]
		sum1 := simd.Popcnt(curSlice)
		sum2 := popcntBytesSlow(curSlice)
		if sum1 != sum2 {
			t.Fatal("Mismatched popcounts (slow).")
		}
	}
}

func countCGSubtask(src []byte, nIter int) int {
	tot := 0
	for iter := 0; iter < nIter; iter++ {
		tot += simd.MaskThenCountByte(src, 0xfb, 'C')
	}
	return tot
}

func countCGSubtaskFuture(src []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- countCGSubtask(src, nIter) }()
	return future
}

func multiCountCG(srcs [][]byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = countCGSubtaskFuture(srcs[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = countCGSubtaskFuture(srcs[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkCountCG(cpus int, nByte int, nJob int, b *testing.B) {
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
		mainSlices[ii] = newArr[1 : nByte+1]
	}
	for i := 0; i < b.N; i++ {
		multiCountCG(mainSlices, cpus, nJob)
	}
}

// Base sequence in length-150 .bam read occupies 75 bytes, so 75 is a good
// size for the short-array benchmark.
func Benchmark_CountCGShort1(b *testing.B) {
	benchmarkCountCG(1, 75, 9999999, b)
}

func Benchmark_CountCGShort4(b *testing.B) {
	benchmarkCountCG(4, 75, 9999999, b)
}

func Benchmark_CountCGShortMax(b *testing.B) {
	benchmarkCountCG(runtime.NumCPU(), 75, 9999999, b)
}

// GRCh37 chromosome 1 length is 249250621, so that's a plausible long-array
// use case.
func Benchmark_CountCGLong1(b *testing.B) {
	benchmarkCountCG(1, 249250621, 50, b)
}

func Benchmark_CountCGLong4(b *testing.B) {
	benchmarkCountCG(4, 249250621, 50, b)
}

func Benchmark_CountCGLongMax(b *testing.B) {
	benchmarkCountCG(runtime.NumCPU(), 249250621, 50, b)
}

var cgArr = [...]byte{'C', 'G'}

func countCGStandard(src []byte) int {
	return bytes.Count(src, cgArr[:1]) + bytes.Count(src, cgArr[1:2])
}

func countCGNaive(src []byte) int {
	cnt := 0
	for _, srcByte := range src {
		// Note that (srcByte & 0xfb) == 'C' takes ~30% less time than this.
		if srcByte == 'C' || srcByte == 'G' {
			cnt++
		}
	}
	return cnt
}

func TestCountCG(t *testing.T) {
	maxSize := 10000
	nIter := 200
	srcArr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		srcSlice := srcArr[sliceStart:sliceEnd]
		for ii := range srcSlice {
			srcSlice[ii] = byte(rand.Intn(256))
		}
		result1 := countCGStandard(srcSlice)
		result2 := simd.MaskThenCountByte(srcSlice, 0xfb, 'C')
		if result1 != result2 {
			t.Fatal("Mismatched MaskThenCountByte result.")
		}
		result2 = countCGNaive(srcSlice)
		if result1 != result2 {
			t.Fatal("Mismatched countCGStandard/countCGNaive results.")
		}
	}
}

func count2BytesStandard(src, vals []byte) int {
	// Not 'Slow' since bytes.Count is decently optimized for a single byte.
	return bytes.Count(src, vals[:1]) + bytes.Count(src, vals[1:2])
}

func TestCount2Bytes(t *testing.T) {
	maxSize := 10000
	nIter := 200
	srcArr := simd.MakeUnsafe(maxSize)
	vals := make([]byte, 2)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		// sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)&^15
		srcSlice := srcArr[sliceStart:sliceEnd]
		for ii := range srcSlice {
			srcSlice[ii] = byte(rand.Intn(256))
		}
		val1 := byte(rand.Intn(256))
		val2 := val1 + 1
		vals[0] = val1
		vals[1] = val2
		result1 := count2BytesStandard(srcSlice, vals)
		result2 := simd.Count2Bytes(srcSlice, val1, val2)
		if result1 != result2 {
			t.Fatal("Mismatched Count2Bytes result.")
		}
	}
}

func count3BytesSubtask(src []byte, nIter int) int {
	tot := 0
	// vals := [...]byte{'A', 'T', 'N'}
	// valsSlice := vals[:]
	for iter := 0; iter < nIter; iter++ {
		tot += simd.Count3Bytes(src, 'A', 'T', 'N')
		// tot += count3BytesStandard(src, valsSlice)
	}
	return tot
}

func count3BytesSubtaskFuture(src []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- count3BytesSubtask(src, nIter) }()
	return future
}

func multiCount3Bytes(srcs [][]byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = count3BytesSubtaskFuture(srcs[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = count3BytesSubtaskFuture(srcs[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkCount3Bytes(cpus int, nByte int, nJob int, b *testing.B) {
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
		multiCount3Bytes(mainSlices, cpus, nJob)
	}
}

// Base sequence in length-150 .bam read occupies 75 bytes, so 75 is a good
// size for the short-array benchmark.
func Benchmark_Count3BytesShort1(b *testing.B) {
	benchmarkCount3Bytes(1, 75, 9999999, b)
}

func Benchmark_Count3BytesShort4(b *testing.B) {
	benchmarkCount3Bytes(4, 75, 9999999, b)
}

func Benchmark_Count3BytesShortMax(b *testing.B) {
	benchmarkCount3Bytes(runtime.NumCPU(), 75, 9999999, b)
}

// GRCh37 chromosome 1 length is 249250621, so that's a plausible long-array
// use case.
func Benchmark_Count3BytesLong1(b *testing.B) {
	benchmarkCount3Bytes(1, 249250621, 50, b)
}

func Benchmark_Count3BytesLong4(b *testing.B) {
	benchmarkCount3Bytes(4, 249250621, 50, b)
}

func Benchmark_Count3BytesLongMax(b *testing.B) {
	benchmarkCount3Bytes(runtime.NumCPU(), 249250621, 50, b)
}

func count3BytesStandard(src, vals []byte) int {
	return bytes.Count(src, vals[:1]) + bytes.Count(src, vals[1:2]) + bytes.Count(src, vals[2:3])
}

func TestCount3Bytes(t *testing.T) {
	maxSize := 10000
	nIter := 200
	srcArr := simd.MakeUnsafe(maxSize)
	vals := make([]byte, 3)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		srcSlice := srcArr[sliceStart:sliceEnd]
		for ii := range srcSlice {
			srcSlice[ii] = byte(rand.Intn(256))
		}
		val1 := byte(rand.Intn(256))
		val2 := val1 + 1
		val3 := val1 + 2
		vals[0] = val1
		vals[1] = val2
		vals[2] = val3
		result1 := count3BytesStandard(srcSlice, vals)
		result2 := simd.Count3Bytes(srcSlice, val1, val2, val3)
		if result1 != result2 {
			t.Fatal("Mismatched Count3Bytes result.")
		}
	}
}

func countNibblesInSetSlow(src []byte, tablePtr *[16]byte) int {
	cnt := 0
	for _, srcByte := range src {
		cnt += int(tablePtr[srcByte&15] + tablePtr[srcByte>>4])
	}
	return cnt
}

func TestCountNibblesInSet(t *testing.T) {
	maxSize := 10000
	nIter := 200
	srcArr := simd.MakeUnsafe(maxSize)
	var table [16]byte
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		srcSlice := srcArr[sliceStart:sliceEnd]
		for ii := range srcSlice {
			srcSlice[ii] = byte(rand.Intn(256))
		}
		baseCode1 := byte(rand.Intn(15))
		baseCode2 := baseCode1 + 1 + byte(rand.Intn(int(15-baseCode1)))
		table[baseCode1] = 1
		table[baseCode2] = 1

		result1 := countNibblesInSetSlow(srcSlice, &table)
		result2 := simd.CountNibblesInSet(srcSlice, &table)
		if result1 != result2 {
			t.Fatal("Mismatched CountNibblesInSet result.")
		}
		table[baseCode1] = 0
		table[baseCode2] = 0
	}
}

func TestCountNibblesInTwoSets(t *testing.T) {
	maxSize := 10000
	nIter := 200
	srcArr := simd.MakeUnsafe(maxSize)
	var table1, table2 [16]byte
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		srcSlice := srcArr[sliceStart:sliceEnd]
		for ii := range srcSlice {
			srcSlice[ii] = byte(rand.Intn(256))
		}
		baseCode1 := byte(rand.Intn(15))
		baseCode2 := baseCode1 + 1 + byte(rand.Intn(int(15-baseCode1)))
		table1[baseCode1] = 1
		table1[baseCode2] = 1

		for ii := 0; ii != 5; ii++ {
			table2[rand.Intn(16)] = 1
		}

		result1a := countNibblesInSetSlow(srcSlice, &table1)
		result1b := countNibblesInSetSlow(srcSlice, &table2)
		result2a, result2b := simd.CountNibblesInTwoSets(srcSlice, &table1, &table2)
		if (result1a != result2a) || (result1b != result2b) {
			t.Fatal("Mismatched CountNibblesInTwoSets result.")
		}
		table1[baseCode1] = 0
		table1[baseCode2] = 0
		for pos := range table2 {
			table2[pos] = 0
		}
	}
}

func countUnpackedNibblesInSetSlow(src []byte, tablePtr *[16]byte) int {
	cnt := 0
	for _, srcByte := range src {
		cnt += int(tablePtr[srcByte])
	}
	return cnt
}

func TestCountUnpackedNibblesInSet(t *testing.T) {
	maxSize := 10000
	nIter := 200
	srcArr := simd.MakeUnsafe(maxSize)
	var table [16]byte
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		srcSlice := srcArr[sliceStart:sliceEnd]
		for ii := range srcSlice {
			srcSlice[ii] = byte(rand.Intn(16))
		}
		baseCode1 := byte(rand.Intn(15))
		baseCode2 := baseCode1 + 1 + byte(rand.Intn(int(15-baseCode1)))
		table[baseCode1] = 1
		table[baseCode2] = 1

		result1 := countUnpackedNibblesInSetSlow(srcSlice, &table)
		result2 := simd.CountUnpackedNibblesInSet(srcSlice, &table)
		if result1 != result2 {
			t.Fatal("Mismatched CountUnpackedNibblesInSet result.")
		}
		table[baseCode1] = 0
		table[baseCode2] = 0
	}
}

func TestCountUnpackedNibblesInTwoSets(t *testing.T) {
	maxSize := 10000
	nIter := 200
	srcArr := simd.MakeUnsafe(maxSize)
	var table1, table2 [16]byte
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		srcSlice := srcArr[sliceStart:sliceEnd]
		for ii := range srcSlice {
			srcSlice[ii] = byte(rand.Intn(16))
		}
		baseCode1 := byte(rand.Intn(15))
		baseCode2 := baseCode1 + 1 + byte(rand.Intn(int(15-baseCode1)))
		table1[baseCode1] = 1
		table1[baseCode2] = 1

		for ii := 0; ii != 5; ii++ {
			table2[rand.Intn(16)] = 1
		}

		result1a := countUnpackedNibblesInSetSlow(srcSlice, &table1)
		result1b := countUnpackedNibblesInSetSlow(srcSlice, &table2)
		result2a, result2b := simd.CountUnpackedNibblesInTwoSets(srcSlice, &table1, &table2)
		if (result1a != result2a) || (result1b != result2b) {
			t.Fatal("Mismatched CountUnpackedNibblesInTwoSets result.")
		}
		table1[baseCode1] = 0
		table1[baseCode2] = 0
		for pos := range table2 {
			table2[pos] = 0
		}
	}
}

func accumulate8Subtask(src []byte, nIter int) int {
	tot := 0
	for iter := 0; iter < nIter; iter++ {
		tot += simd.Accumulate8(src)
		// tot += accumulate8Slow(src)
	}
	return tot
}

func accumulate8SubtaskFuture(src []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- accumulate8Subtask(src, nIter) }()
	return future
}

func multiAccumulate8(srcs [][]byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = accumulate8SubtaskFuture(srcs[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = accumulate8SubtaskFuture(srcs[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkAccumulate8(cpus int, nByte int, nJob int, b *testing.B) {
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
		multiAccumulate8(mainSlices, cpus, nJob)
	}
}

// Base sequence in length-150 .bam read occupies 75 bytes, so 75 is a good
// size for the short-array benchmark.
func Benchmark_Accumulate8Short1(b *testing.B) {
	benchmarkAccumulate8(1, 75, 9999999, b)
}

func Benchmark_Accumulate8Short4(b *testing.B) {
	benchmarkAccumulate8(4, 75, 9999999, b)
}

func Benchmark_Accumulate8ShortMax(b *testing.B) {
	benchmarkAccumulate8(runtime.NumCPU(), 75, 9999999, b)
}

// GRCh37 chromosome 1 length is 249250621, so that's a plausible long-array
// use case.
func Benchmark_Accumulate8Long1(b *testing.B) {
	benchmarkAccumulate8(1, 249250621, 50, b)
}

func Benchmark_Accumulate8Long4(b *testing.B) {
	benchmarkAccumulate8(4, 249250621, 50, b)
}

func Benchmark_Accumulate8LongMax(b *testing.B) {
	benchmarkAccumulate8(runtime.NumCPU(), 249250621, 50, b)
}

func accumulate8Slow(src []byte) int {
	cnt := 0
	for _, srcByte := range src {
		cnt += int(srcByte)
	}
	return cnt
}

func TestAccumulate8(t *testing.T) {
	maxSize := 500
	nIter := 200
	srcArr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		srcSlice := srcArr[sliceStart:sliceEnd]
		for ii := range srcSlice {
			srcSlice[ii] = byte(rand.Intn(256))
		}

		result1 := accumulate8Slow(srcSlice)
		result2 := simd.Accumulate8(srcSlice)
		if result1 != result2 {
			t.Fatal("Mismatched Accumulate8 result.")
		}
	}
}

func accumulate8GreaterSubtask(src []byte, nIter int) int {
	tot := 0
	for iter := 0; iter < nIter; iter++ {
		tot += simd.Accumulate8Greater(src, 14)
		// tot += accumulate8GreaterSlow(src, 14)
	}
	return tot
}

func accumulate8GreaterSubtaskFuture(src []byte, nIter int) chan int {
	future := make(chan int)
	go func() { future <- accumulate8GreaterSubtask(src, nIter) }()
	return future
}

func multiAccumulate8Greater(srcs [][]byte, cpus int, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSizeBase := nJob / cpus
	shardRemainder := nJob - shardSizeBase*cpus
	shardSizeP1 := shardSizeBase + 1
	var taskIdx int
	for ; taskIdx < shardRemainder; taskIdx++ {
		sumFutures[taskIdx] = accumulate8GreaterSubtaskFuture(srcs[taskIdx], shardSizeP1)
	}
	for ; taskIdx < cpus; taskIdx++ {
		sumFutures[taskIdx] = accumulate8GreaterSubtaskFuture(srcs[taskIdx], shardSizeBase)
	}
	var sum int
	for taskIdx = 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

func benchmarkAccumulate8Greater(cpus int, nByte int, nJob int, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	mainSlices := make([][]byte, cpus)
	for ii := range mainSlices {
		// Add 63 to prevent false sharing.
		newArr := simd.MakeUnsafe(nByte + 63)
		for jj := 0; jj < nByte; jj++ {
			newArr[jj] = byte(jj*3) & 127
		}
		mainSlices[ii] = newArr[:nByte]
	}
	for i := 0; i < b.N; i++ {
		multiAccumulate8Greater(mainSlices, cpus, nJob)
	}
}

// Base sequence in length-150 .bam read occupies 75 bytes, so 75 is a good
// size for the short-array benchmark.
func Benchmark_Accumulate8GreaterShort1(b *testing.B) {
	benchmarkAccumulate8Greater(1, 75, 9999999, b)
}

func Benchmark_Accumulate8GreaterShort4(b *testing.B) {
	benchmarkAccumulate8Greater(4, 75, 9999999, b)
}

func Benchmark_Accumulate8GreaterShortMax(b *testing.B) {
	benchmarkAccumulate8Greater(runtime.NumCPU(), 75, 9999999, b)
}

// GRCh37 chromosome 1 length is 249250621, so that's a plausible long-array
// use case.
func Benchmark_Accumulate8GreaterLong1(b *testing.B) {
	benchmarkAccumulate8Greater(1, 249250621, 50, b)
}

func Benchmark_Accumulate8GreaterLong4(b *testing.B) {
	benchmarkAccumulate8Greater(4, 249250621, 50, b)
}

func Benchmark_Accumulate8GreaterLongMax(b *testing.B) {
	benchmarkAccumulate8Greater(runtime.NumCPU(), 249250621, 50, b)
}

func accumulate8GreaterSlow(src []byte, val byte) int {
	cnt := 0
	for _, srcByte := range src {
		if srcByte > val {
			cnt += int(srcByte)
		}
	}
	return cnt
}

func TestAccumulate8Greater(t *testing.T) {
	maxSize := 500
	nIter := 200
	srcArr := simd.MakeUnsafe(maxSize)
	for iter := 0; iter < nIter; iter++ {
		sliceStart := rand.Intn(maxSize)
		sliceEnd := sliceStart + rand.Intn(maxSize-sliceStart)
		srcSlice := srcArr[sliceStart:sliceEnd]
		for ii := range srcSlice {
			srcSlice[ii] = byte(rand.Intn(256))
		}

		val := byte(rand.Intn(256))

		result1 := accumulate8GreaterSlow(srcSlice, val)
		result2 := simd.Accumulate8Greater(srcSlice, val)
		if result1 != result2 {
			t.Fatal("Mismatched Accumulate8Greater result.")
		}
	}
}
