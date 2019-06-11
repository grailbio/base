// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package simd_test

import (
	"bytes"
	"math/bits"
	"math/rand"
	"reflect"
	"testing"
	"unsafe"

	"github.com/grailbio/base/simd"
)

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

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_Popcnt/SIMDShort1Cpu-8                      20          90993141 ns/op
Benchmark_Popcnt/SIMDShortHalfCpu-8                   50          24639468 ns/op
Benchmark_Popcnt/SIMDShortAllCpu-8                   100          23098747 ns/op
Benchmark_Popcnt/SIMDLong1Cpu-8                        2         909927976 ns/op
Benchmark_Popcnt/SIMDLongHalfCpu-8                     3         488961048 ns/op
Benchmark_Popcnt/SIMDLongAllCpu-8                      3         466249901 ns/op
Benchmark_Popcnt/NoasmShort1Cpu-8                     10         106873386 ns/op
Benchmark_Popcnt/NoasmShortHalfCpu-8                  50          29290668 ns/op
Benchmark_Popcnt/NoasmShortAllCpu-8                   50          29559455 ns/op
Benchmark_Popcnt/NoasmLong1Cpu-8                       1        1217844097 ns/op
Benchmark_Popcnt/NoasmLongHalfCpu-8                    2         507946501 ns/op
Benchmark_Popcnt/NoasmLongAllCpu-8                     3         483458386 ns/op
Benchmark_Popcnt/SlowShort1Cpu-8                       2         519449562 ns/op
Benchmark_Popcnt/SlowShortHalfCpu-8                   10         139108095 ns/op
Benchmark_Popcnt/SlowShortAllCpu-8                    10         143346876 ns/op
Benchmark_Popcnt/SlowLong1Cpu-8                        1        7515831696 ns/op
Benchmark_Popcnt/SlowLongHalfCpu-8                     1        2083880380 ns/op
Benchmark_Popcnt/SlowLongAllCpu-8                      1        2064129411 ns/op

Notes: The current SSE4.2 SIMD implementation just amounts to a 2x-unrolled
OnesCount64 loop without flag-rechecking overhead; they're using the same
underlying instruction.  AVX2/AVX-512 allow for faster bulk processing, though;
see e.g. https://github.com/kimwalisch/libpopcnt .
*/

func popcntSimdSubtask(dst, src []byte, nIter int) int {
	sum := 0
	for iter := 0; iter < nIter; iter++ {
		sum += simd.Popcnt(src)
	}
	return sum
}

func popcntNoasmSubtask(dst, src []byte, nIter int) int {
	sum := 0
	for iter := 0; iter < nIter; iter++ {
		sum += popcntBytesNoasm(src)
	}
	return sum
}

func popcntSlowSubtask(dst, src []byte, nIter int) int {
	sum := 0
	for iter := 0; iter < nIter; iter++ {
		sum += popcntBytesSlow(src)
	}
	return sum
}

func Benchmark_Popcnt(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   popcntSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   popcntNoasmSubtask,
			tag: "Noasm",
		},
		{
			f:   popcntSlowSubtask,
			tag: "Slow",
		},
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Short", 0, 75, 9999999, b)
		multiBenchmark(f.f, f.tag+"Long", 0, 249250621, 50, b)
	}
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

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_CountCG/SIMDShort1Cpu-8                     10         119280079 ns/op
Benchmark_CountCG/SIMDShortHalfCpu-8                  50          34743805 ns/op
Benchmark_CountCG/SIMDShortAllCpu-8                   50          28507338 ns/op
Benchmark_CountCG/SIMDLong1Cpu-8                       2         765099599 ns/op
Benchmark_CountCG/SIMDLongHalfCpu-8                    3         491655239 ns/op
Benchmark_CountCG/SIMDLongAllCpu-8                     3         452592924 ns/op
Benchmark_CountCG/StandardShort1Cpu-8                  5         237081120 ns/op
Benchmark_CountCG/StandardShortHalfCpu-8              20          64949969 ns/op
Benchmark_CountCG/StandardShortAllCpu-8               20          59167932 ns/op
Benchmark_CountCG/StandardLong1Cpu-8                   1        1496389230 ns/op
Benchmark_CountCG/StandardLongHalfCpu-8                2         931898463 ns/op
Benchmark_CountCG/StandardLongAllCpu-8                 2         980615182 ns/op
*/

func countCGSimdSubtask(dst, src []byte, nIter int) int {
	tot := 0
	for iter := 0; iter < nIter; iter++ {
		tot += simd.MaskThenCountByte(src, 0xfb, 'C')
	}
	return tot
}

func countCGStandardSubtask(dst, src []byte, nIter int) int {
	tot := 0
	for iter := 0; iter < nIter; iter++ {
		tot += countCGStandard(src)
	}
	return tot
}

func Benchmark_CountCG(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   countCGSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   countCGStandardSubtask,
			tag: "Standard",
		},
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Short", 0, 150, 9999999, b)
		multiBenchmark(f.f, f.tag+"Long", 0, 249250621, 50, b)
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

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_Count3Bytes/SIMDShort1Cpu-8                 10         141085860 ns/op
Benchmark_Count3Bytes/SIMDShortHalfCpu-8              30          40371892 ns/op
Benchmark_Count3Bytes/SIMDShortAllCpu-8               30          37769995 ns/op
Benchmark_Count3Bytes/SIMDLong1Cpu-8                   2         945534510 ns/op
Benchmark_Count3Bytes/SIMDLongHalfCpu-8                3         499146889 ns/op
Benchmark_Count3Bytes/SIMDLongAllCpu-8                 3         475811932 ns/op
Benchmark_Count3Bytes/StandardShort1Cpu-8              3         346637595 ns/op
Benchmark_Count3Bytes/StandardShortHalfCpu-8          20          96524251 ns/op
Benchmark_Count3Bytes/StandardShortAllCpu-8           20          87056185 ns/op
Benchmark_Count3Bytes/StandardLong1Cpu-8               1        2260954596 ns/op
Benchmark_Count3Bytes/StandardLongHalfCpu-8            1        1518757560 ns/op
Benchmark_Count3Bytes/StandardLongAllCpu-8             1        1468352229 ns/op
*/

func count3BytesSimdSubtask(dst, src []byte, nIter int) int {
	tot := 0
	for iter := 0; iter < nIter; iter++ {
		tot += simd.Count3Bytes(src, 'A', 'T', 'N')
	}
	return tot
}

func count3BytesStandardSubtask(dst, src []byte, nIter int) int {
	tot := 0
	vals := []byte{'A', 'T', 'N'}
	for iter := 0; iter < nIter; iter++ {
		tot += count3BytesStandard(src, vals)
	}
	return tot
}

func Benchmark_Count3Bytes(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   count3BytesSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   count3BytesStandardSubtask,
			tag: "Standard",
		},
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Short", 0, 150, 9999999, b)
		multiBenchmark(f.f, f.tag+"Long", 0, 249250621, 50, b)
	}
}

func countNibblesInSetSlow(src []byte, tablePtr *simd.NibbleLookupTable) int {
	cnt := 0
	for _, srcByte := range src {
		cnt += int(tablePtr.Get(srcByte&15) + tablePtr.Get(srcByte>>4))
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
		nlt := simd.MakeNibbleLookupTable(table)

		result1 := countNibblesInSetSlow(srcSlice, &nlt)
		result2 := simd.CountNibblesInSet(srcSlice, &nlt)
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
		nlt1 := simd.MakeNibbleLookupTable(table1)
		nlt2 := simd.MakeNibbleLookupTable(table2)

		result1a := countNibblesInSetSlow(srcSlice, &nlt1)
		result1b := countNibblesInSetSlow(srcSlice, &nlt2)
		result2a, result2b := simd.CountNibblesInTwoSets(srcSlice, &nlt1, &nlt2)
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

func countUnpackedNibblesInSetSlow(src []byte, tablePtr *simd.NibbleLookupTable) int {
	cnt := 0
	for _, srcByte := range src {
		cnt += int(tablePtr.Get(srcByte))
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
		nlt := simd.MakeNibbleLookupTable(table)

		result1 := countUnpackedNibblesInSetSlow(srcSlice, &nlt)
		result2 := simd.CountUnpackedNibblesInSet(srcSlice, &nlt)
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
		nlt1 := simd.MakeNibbleLookupTable(table1)
		nlt2 := simd.MakeNibbleLookupTable(table2)

		result1a := countUnpackedNibblesInSetSlow(srcSlice, &nlt1)
		result1b := countUnpackedNibblesInSetSlow(srcSlice, &nlt2)
		result2a, result2b := simd.CountUnpackedNibblesInTwoSets(srcSlice, &nlt1, &nlt2)
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

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_Accumulate8/SIMDShort1Cpu-8                 20          92560842 ns/op
Benchmark_Accumulate8/SIMDShortHalfCpu-8              50          24796260 ns/op
Benchmark_Accumulate8/SIMDShortAllCpu-8              100          21541910 ns/op
Benchmark_Accumulate8/SIMDLong1Cpu-8                   2         778781187 ns/op
Benchmark_Accumulate8/SIMDLongHalfCpu-8                3         466101270 ns/op
Benchmark_Accumulate8/SIMDLongAllCpu-8                 3         472125495 ns/op
Benchmark_Accumulate8/SlowShort1Cpu-8                  2         725211331 ns/op
Benchmark_Accumulate8/SlowShortHalfCpu-8              10         192303935 ns/op
Benchmark_Accumulate8/SlowShortAllCpu-8               10         146159760 ns/op
Benchmark_Accumulate8/SlowLong1Cpu-8                   1        5371110621 ns/op
Benchmark_Accumulate8/SlowLongHalfCpu-8                1        1473946277 ns/op
Benchmark_Accumulate8/SlowLongAllCpu-8                 1        1118962315 ns/op
*/

func accumulate8SimdSubtask(dst, src []byte, nIter int) int {
	tot := 0
	for iter := 0; iter < nIter; iter++ {
		tot += simd.Accumulate8(src)
	}
	return tot
}

func accumulate8SlowSubtask(dst, src []byte, nIter int) int {
	tot := 0
	for iter := 0; iter < nIter; iter++ {
		tot += accumulate8Slow(src)
	}
	return tot
}

func Benchmark_Accumulate8(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   accumulate8SimdSubtask,
			tag: "SIMD",
		},
		{
			f:   accumulate8SlowSubtask,
			tag: "Slow",
		},
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Short", 0, 150, 9999999, b)
		multiBenchmark(f.f, f.tag+"Long", 0, 249250621, 50, b)
	}
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

/*
Benchmark results:
  MacBook Pro (15-inch, 2016)
  2.7 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3

Benchmark_Accumulate8Greater/SIMDShort1Cpu-8                  10         137436870 ns/op
Benchmark_Accumulate8Greater/SIMDShortHalfCpu-8               50          36257710 ns/op
Benchmark_Accumulate8Greater/SIMDShortAllCpu-8                50          32131334 ns/op
Benchmark_Accumulate8Greater/SIMDLong1Cpu-8                    2         895831574 ns/op
Benchmark_Accumulate8Greater/SIMDLongHalfCpu-8                 2         501501504 ns/op
Benchmark_Accumulate8Greater/SIMDLongAllCpu-8                  3         473122019 ns/op
Benchmark_Accumulate8Greater/SlowShort1Cpu-8                   1        1026311714 ns/op
Benchmark_Accumulate8Greater/SlowShortHalfCpu-8                5         270841153 ns/op
Benchmark_Accumulate8Greater/SlowShortAllCpu-8                 5         254131935 ns/op
Benchmark_Accumulate8Greater/SlowLong1Cpu-8                    1        7651910478 ns/op
Benchmark_Accumulate8Greater/SlowLongHalfCpu-8                 1        2113221447 ns/op
Benchmark_Accumulate8Greater/SlowLongAllCpu-8                  1        2047822921 ns/op
*/

func accumulate8GreaterSimdSubtask(dst, src []byte, nIter int) int {
	tot := 0
	for iter := 0; iter < nIter; iter++ {
		tot += simd.Accumulate8Greater(src, 14)
	}
	return tot
}

func accumulate8GreaterSlowSubtask(dst, src []byte, nIter int) int {
	tot := 0
	for iter := 0; iter < nIter; iter++ {
		tot += accumulate8GreaterSlow(src, 14)
	}
	return tot
}

func Benchmark_Accumulate8Greater(b *testing.B) {
	funcs := []taggedMultiBenchFunc{
		{
			f:   accumulate8GreaterSimdSubtask,
			tag: "SIMD",
		},
		{
			f:   accumulate8GreaterSlowSubtask,
			tag: "Slow",
		},
	}
	for _, f := range funcs {
		multiBenchmark(f.f, f.tag+"Short", 0, 150, 9999999, b)
		multiBenchmark(f.f, f.tag+"Long", 0, 249250621, 50, b)
	}
}
