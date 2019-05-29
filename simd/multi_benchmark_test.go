// Copyright 2019 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package simd_test

import (
	"runtime"
	"testing"

	"github.com/grailbio/base/simd"
)

// Utility functions to assist with benchmarking of embarrassingly parallel
// jobs.

type multiBenchFunc func(args interface{}, nIter int) int

func multiBenchmark(bf multiBenchFunc, argSlice []interface{}, cpus, nJob int) {
	sumFutures := make([]chan int, cpus)
	shardSize := 1 + (nJob / cpus)
	shardRemainder := nJob % cpus
	for taskIdx := 0; taskIdx < cpus; taskIdx++ {
		if taskIdx == shardRemainder {
			shardSize--
		}
		sumFutures[taskIdx] = func(bf multiBenchFunc, args interface{}, nIter int) chan int {
			future := make(chan int)
			go func() { future <- bf(args, nIter) }()
			return future
		}(bf, argSlice[taskIdx], shardSize)
	}
	var sum int
	for taskIdx := 0; taskIdx < cpus; taskIdx++ {
		sum += <-sumFutures[taskIdx]
	}
}

type dstSrcArgs struct {
	dst []byte
	src []byte
}

func multiBenchmarkDstSrc(bf multiBenchFunc, cpus int, nDstByte, nSrcByte, nJob int, b *testing.B) {
	if cpus > runtime.NumCPU() {
		b.Skipf("only have %v cpus", runtime.NumCPU())
	}

	var argSlice []interface{}
	for i := 0; i < cpus; i++ {
		// Add 63 to prevent false sharing.
		newArrSrc := simd.MakeUnsafe(nSrcByte + 63)
		for j := 0; j < nSrcByte; j++ {
			newArrSrc[j] = byte(j * 3)
		}
		newArrDst := simd.MakeUnsafe(nDstByte + 63)
		newArgs := dstSrcArgs{
			dst: newArrDst[:nDstByte],
			src: newArrSrc[:nSrcByte],
		}
		argSlice = append(argSlice, newArgs)
	}
	for i := 0; i < b.N; i++ {
		multiBenchmark(bf, argSlice, cpus, nJob)
	}
}
