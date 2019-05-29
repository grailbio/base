// Copyright 2019 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package simd_test

import (
	"runtime"
	"testing"

	"github.com/grailbio/base/simd"
	"github.com/grailbio/base/traverse"
)

// Utility functions to assist with benchmarking of embarrassingly parallel
// jobs.

type multiBenchFunc func(args interface{}, nIter int) int

type taggedMultiBenchFunc struct {
	f   multiBenchFunc
	tag string
}

// May want to replace this with something based on testing.B's RunParallel
// method.  (Haven't done so yet since I don't see a clean way to make that
// play well with per-core preallocated buffers.)
func multiBenchmark(bf multiBenchFunc, argSlice []interface{}, parallelism, nJob int) {
	// 'bf' is expected to execute the benchmarking target nIter times.
	//
	// Given that, multiBenchmark launches 'parallelism' goroutines, where each
	// goroutine has nIter set to roughly (nJob / parallelism), so that the total
	// number of benchmark-target-function invocations across all threads is
	// nJob.  It is designed to measure how effective traverse.Each-style
	// parallelization is at reducing wall-clock runtime.
	_ = traverse.Each(parallelism, func(threadIdx int) error {
		nIter := (((threadIdx + 1) * nJob) / parallelism) - ((threadIdx * nJob) / parallelism)
		_ = bf(argSlice[threadIdx], nIter)
		return nil
	})
}

type dstSrcArgs struct {
	dst []byte
	src []byte
}

func multiBenchmarkDstSrc(bf multiBenchFunc, benchmarkSubtype string, nDstByte, nSrcByte, nJob int, b *testing.B) {
	totalCpu := runtime.NumCPU()
	cases := []struct {
		nCpu    int
		descrip string
	}{
		{
			nCpu:    1,
			descrip: "1Cpu",
		},
		// 'Half' is often the saturation point, due to hyperthreading.
		{
			nCpu:    (totalCpu + 1) / 2,
			descrip: "HalfCpu",
		},
		{
			nCpu:    totalCpu,
			descrip: "AllCpu",
		},
	}
	for _, c := range cases {
		success := b.Run(benchmarkSubtype+c.descrip, func(b *testing.B) {
			var argSlice []interface{}
			for i := 0; i < c.nCpu; i++ {
				// Add 63 to prevent false sharing.
				newArrSrc := simd.MakeUnsafe(nSrcByte + 63)
				if i == 0 {
					for j := 0; j < nSrcByte; j++ {
						newArrSrc[j] = byte(j * 3)
					}
				} else {
					copy(newArrSrc[:nSrcByte], argSlice[0].(dstSrcArgs).src)
				}
				newArrDst := simd.MakeUnsafe(nDstByte + 63)
				newArgs := dstSrcArgs{
					dst: newArrDst[:nDstByte],
					src: newArrSrc[:nSrcByte],
				}
				argSlice = append(argSlice, newArgs)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				multiBenchmark(bf, argSlice, c.nCpu, nJob)
			}
		})
		if !success {
			panic("benchmark failed")
		}
	}
}
