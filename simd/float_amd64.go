// Copyright 2021 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build amd64,!appengine

package simd

import (
	"math"
	"reflect"
	"unsafe"

	"golang.org/x/sys/cpu"
)

//go:noescape
func findNaNOrInf64SSSE3Asm(data unsafe.Pointer, nElem int) int

//go:noescape
func findNaNOrInf64AVX2Asm(data unsafe.Pointer, nElem int) int

var avx2Available bool

func init() {
	avx2Available = cpu.X86.HasAVX2
	// possible todo: detect FMA and/or AVX512DQ.
}

// FindNaNOrInf64 returns the position of the first NaN/inf value if one is
// present, and -1 otherwise.
func FindNaNOrInf64(data []float64) int {
	nElem := len(data)
	if nElem < 16 {
		for i, x := range data {
			if (math.Float64bits(x) & (0x7ff << 52)) == (0x7ff << 52) {
				return i
			}
		}
		return -1
	}
	dataHeader := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	if avx2Available {
		return findNaNOrInf64AVX2Asm(unsafe.Pointer(dataHeader.Data), nElem)
	} else {
		return findNaNOrInf64SSSE3Asm(unsafe.Pointer(dataHeader.Data), nElem)
	}
}
