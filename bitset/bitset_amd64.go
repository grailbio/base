// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build amd64,!appengine

// This is similar to github.com/willf/bitset , but with some extraneous
// abstraction removed.  See also simd/count_amd64.go.
//
// ([]byte <-> []uintptr adapters will be added when needed.)

package bitset

import (
	"math/bits"
)

// BitsPerWord is the number of bits in a machine word.
const BitsPerWord = 64

// Set sets the given bit in a []uintptr bitset.
func Set(data []uintptr, bitIdx int) {
	// Unsigned division by a power-of-2 constant compiles to a right-shift,
	// while signed does not due to negative nastiness.
	data[uint(bitIdx)/BitsPerWord] |= 1 << (uint(bitIdx) % BitsPerWord)
}

// Clear clears the given bit in a []uintptr bitset.
func Clear(data []uintptr, bitIdx int) {
	wordIdx := uint(bitIdx) / BitsPerWord
	data[wordIdx] = data[wordIdx] &^ (1 << (uint(bitIdx) % BitsPerWord))
}

// Test returns true iff the given bit is set.
func Test(data []uintptr, bitIdx int) bool {
	return (data[uint(bitIdx)/BitsPerWord] & (1 << (uint(bitIdx) % BitsPerWord))) != 0
}

// NonzeroWordScanner iterates over and clears the set bits in a bitset, with
// the somewhat unusual precondition that the number of nonzero words is known
// in advance.  The 'BitsetScanner' name is being reserved for a scanner which
// expects the number of set bits to be known instead.
//
// Note that, when many bits are set, a more complicated double-loop based
// around a function like willf/bitset.NextSetMany() has ~40% less overhead (at
// least with Go 1.10 on a Mac), and you can do even better with manual
// inlining of the iteration logic.  As a consequence, it shouldn't be used
// when the bit iteration/clearing process is actually the dominant
// computational cost (and neither should NextSetMany(), manual inlining is
// 2-6x better without much more code, see bitsetManualInlineSubtask() in
// bitset_test.go for an example).  However, it's a good choice everywhere
// else, outperforming the other scanners I'm aware of with similar ease of
// use, and maybe a future Go version will inline it properly.
type NonzeroWordScanner struct {
	// data is the original bitset.
	data []uintptr
	// bitIdxOffset is BitsPerWord times the current data[] array index.
	bitIdxOffset int
	// bitWord is bits[bitIdxOffset / BitsPerWord], with already-iterated-over
	// bits cleared.
	bitWord uintptr
	// nNonzeroWord is the number of nonzero words remaining in data[].
	nNonzeroWord int
}

// NewNonzeroWordScanner returns a NonzeroWordScanner for the given bitset,
// along with the position of the first bit.  (This interface has been chosen
// to make for loops with properly-scoped variables easy to write.)
//
// The bitset is expected to be nonempty; otherwise this will crash the program
// with an out-of-bounds slice access.  Similarly, if nNonzeroWord is larger
// than the actual number of nonzero words, or initially <= 0, the standard for
// loop will crash the program.  (If nNonzeroWord is smaller but >0, the last
// nonzero words will be ignored.)
func NewNonzeroWordScanner(data []uintptr, nNonzeroWord int) (NonzeroWordScanner, int) {
	for wordIdx := 0; ; wordIdx++ {
		bitWord := data[wordIdx]
		if bitWord != 0 {
			bitIdxOffset := wordIdx * BitsPerWord
			return NonzeroWordScanner{
				data:         data,
				bitIdxOffset: bitIdxOffset,
				bitWord:      bitWord & (bitWord - 1),
				nNonzeroWord: nNonzeroWord,
			}, bits.TrailingZeros64(uint64(bitWord)) + bitIdxOffset
		}
	}
}

// Next returns the position of the next set bit, or -1 if there aren't any.
func (s *NonzeroWordScanner) Next() int {
	bitWord := s.bitWord
	if bitWord == 0 {
		wordIdx := int(uint(s.bitIdxOffset) / BitsPerWord)
		s.data[wordIdx] = 0
		s.nNonzeroWord--
		if s.nNonzeroWord == 0 {
			// All words with set bits are accounted for, we can exit early.
			// This is deliberately == 0 instead of <= 0 since it'll only be less
			// than zero if there's a bug in the caller.  We want to crash with an
			// out-of-bounds access in that case.
			return -1
		}
		for {
			wordIdx++
			bitWord = s.data[wordIdx]
			if bitWord != 0 {
				break
			}
		}
		s.bitIdxOffset = wordIdx * BitsPerWord
	}
	s.bitWord = bitWord & (bitWord - 1)
	return bits.TrailingZeros64(uint64(bitWord)) + s.bitIdxOffset
}
