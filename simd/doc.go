// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package simd provides access to SIMD-based implementations of several common
// operations on byte arrays which the compiler cannot be trusted to
// autovectorize within the next several years.
//
// The backend currently assumes SSE4.2 is available, and does not use anything
// past that.  (init() checks for SSE4.2 support, and panics when it isn't
// there.)  However, the interface is designed to allow the backend to
// autodetect e.g. AVX2/AVX-512 and opportunistically use those instructions,
// without any changes to properly written higher-level code.
//
//
// The central constraint driving this package's design is the standard Go
// compiler's inability to inline short assembly functions; see
//   https://groups.google.com/forum/#!topic/golang-nuts/yVOfeHYCIT4
//   https://github.com/golang/go/issues/4947#issuecomment-66075571
// for more details.  As a consequence, it is critical to push looping logic
// down to the assembly level as well, otherwise function call overhead is
// overwhelming.  Conversely, due to the much higher development burden of this
// type of code, we don't go beyond that point; this package is essentially a
// collection of for loops.
//
// Two classes of functions are exported:
//
// - Functions with 'Unsafe' in their names will assume it is safe to use the
// main vectorized loop to process the entire slice; this may involve memory
// accesses a few bytes beyond the end of the slice.  MakeUnsafe() and related
// functions can be used to allocate a slice with sufficient capacity for this
// to work (this currently means bytesPerVec extra bytes; simply rounding up to
// a multiple of bytesPerVec is not always enough).  They may have other
// preconditions as well, and won't check those, either.
//
// - Their safe analogues work properly on ordinary slices, and often panic
// when documented preconditions are not met.  When a precondition is not
// explicitly checked (due to computational cost), safe functions may return
// garbage values when the condition is not met, but they will not corrupt
// unrelated memory or perform out-of-bounds read operations.  (Unsafe
// functions may do either of those things when misused.)
package simd
