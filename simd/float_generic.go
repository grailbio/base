// Copyright 2021 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build !amd64 appengine

package simd

import (
	"math"
)

// FindNaNOrInf64 returns the position of the first NaN/inf value if one is
// present, and -1 otherwise.
func FindNaNOrInf64(data []float64) int {
	for i, x := range data {
		// Extract the exponent bits, and check if they're all set: that (and only
		// that) corresponds to NaN/inf.
		if (math.Float64bits(x) & (0x7ff << 52)) == (0x7ff << 52) {
			return i
		}
	}
	return -1
}
