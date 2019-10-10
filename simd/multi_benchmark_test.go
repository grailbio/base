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
// jobs. It probably makes sense to move this code to a more central location
// at some point.

type multiBenchFunc func(dst, src []byte, nIter int) int

type taggedMultiBenchFunc struct {
	f   multiBenchFunc
	tag string
}

type bytesInitFunc func(src []byte)

type multiBenchmarkOpts struct {
	dstInit bytesInitFunc
	srcInit bytesInitFunc
}

func multiBenchmark(bf multiBenchFunc, benchmarkSubtype string, nDstByte, nSrcByte, nJob int, b *testing.B, opts ...multiBenchmarkOpts) {
	// 'bf' is expected to execute the benchmarking target nIter times.
	//
	// Given that, for each of the 3 nCpu settings below, multiBenchmark launches
	// 'parallelism' goroutines, where each goroutine has nIter set to roughly
	// (nJob / nCpu), so that the total number of benchmark-target-function
	// invocations across all threads is nJob.  It is designed to measure how
	// effective traverse.Each-style parallelization is at reducing wall-clock
	// runtime.
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
	var dstInit bytesInitFunc
	var srcInit bytesInitFunc
	if len(opts) >= 1 {
		dstInit = opts[0].dstInit
		srcInit = opts[0].srcInit
	}
	for _, c := range cases {
		success := b.Run(benchmarkSubtype+c.descrip, func(b *testing.B) {
			dsts := make([][]byte, c.nCpu)
			srcs := make([][]byte, c.nCpu)
			for i := 0; i < c.nCpu; i++ {
				// Add 63 to prevent false sharing.
				newArrDst := simd.MakeUnsafe(nDstByte + 63)
				newArrSrc := simd.MakeUnsafe(nSrcByte + 63)
				if i == 0 {
					if dstInit != nil {
						dstInit(newArrDst)
					}
					if srcInit != nil {
						srcInit(newArrSrc)
					} else {
						for j := 0; j < nSrcByte; j++ {
							newArrSrc[j] = byte(j * 3)
						}
					}
				} else {
					if dstInit != nil {
						copy(newArrDst[:nDstByte], dsts[0])
					}
					copy(newArrSrc[:nSrcByte], srcs[0])
				}
				dsts[i] = newArrDst[:nDstByte]
				srcs[i] = newArrSrc[:nSrcByte]
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// May want to replace this with something based on testing.B's
				// RunParallel method.  (Haven't done so yet since I don't see a clean
				// way to make that play well with per-core preallocated buffers.)
				_ = traverse.Each(c.nCpu, func(threadIdx int) error {
					nIter := (((threadIdx + 1) * nJob) / c.nCpu) - ((threadIdx * nJob) / c.nCpu)
					_ = bf(dsts[threadIdx], srcs[threadIdx], nIter)
					return nil
				})
			}
		})
		if !success {
			panic("benchmark failed")
		}
	}
}

func bytesInit0(src []byte) {
	// do nothing
}

func bytesInitMax15(src []byte) {
	for i := 0; i < len(src); i++ {
		src[i] = byte(i*3) & 15
	}
}

type multiBenchVarargsFunc func(args interface{}, nIter int) int

type taggedMultiBenchVarargsFunc struct {
	f   multiBenchVarargsFunc
	tag string
}

type varargsFactory func() interface{}

func multiBenchmarkVarargs(bvf multiBenchVarargsFunc, benchmarkSubtype string, nJob int, argsFactory varargsFactory, b *testing.B) {
	totalCpu := runtime.NumCPU()
	cases := []struct {
		nCpu    int
		descrip string
	}{
		{
			nCpu:    1,
			descrip: "1Cpu",
		},
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
				// Can take an "args interface{}" parameter and make deep copies
				// instead.
				argSlice = append(argSlice, argsFactory())
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = traverse.Each(c.nCpu, func(threadIdx int) error {
					nIter := (((threadIdx + 1) * nJob) / c.nCpu) - ((threadIdx * nJob) / c.nCpu)
					_ = bvf(argSlice[threadIdx], nIter)
					return nil
				})
			}
		})
		if !success {
			panic("benchmark failed")
		}
	}
}
