// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.
package traverse_test

import (
	"math/rand"

	"github.com/grailbio/base/traverse"
)

func Example() {
	// Compute N random numbers in parallel.
	const N = 1e5
	out := make([]float64, N)
	traverse.Parallel.Range(len(out), func(start, end int) error {
		r := rand.New(rand.NewSource(rand.Int63()))
		for i := start; i < end; i++ {
			out[i] = r.Float64()
		}
		return nil
	})
}
