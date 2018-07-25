// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package data provides functionality for measuring and displaying
// data quantities.
package data

import "fmt"

// A Size represents a data quantity in number of bytes.
type Size int64

// Common data quantities.
const (
	B Size = 1 << (10 * iota)
	KiB
	MiB
	GiB
	TiB
	PiB
	EiB
)

// Bytes returns the size as an integer byte count.
func (s Size) Bytes() int64 {
	return int64(s)
}

// Count returns the number of us in s.
func (s Size) Count(u Size) float64 {
	return float64(s) / float64(u)
}

// String returns a string representation of the data quantity b,
// picking the largest appropriate unit.
func (s Size) String() string {
	abs := s
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs >= EiB:
		return fmt.Sprintf("%.1fEiB", s.Count(EiB))
	case abs >= PiB:
		return fmt.Sprintf("%.1fPiB", s.Count(PiB))
	case abs >= TiB:
		return fmt.Sprintf("%.1fTiB", s.Count(TiB))
	case abs >= GiB:
		return fmt.Sprintf("%.1fGiB", s.Count(GiB))
	case abs >= MiB:
		return fmt.Sprintf("%.1fMiB", s.Count(MiB))
	case abs >= KiB:
		return fmt.Sprintf("%.1fKiB", s.Count(KiB))
	}
	return fmt.Sprintf("%dB", s)
}
