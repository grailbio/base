// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package deprecated

import "io"

// RangeReader represents an io.ReadSeeker that operates over a restricted
// range of the supplied ReadSeeker. Reading and Seeking are consequently
// relative to the start of the range used to create the RangeReader.
type RangeReader struct {
	rs              io.ReadSeeker
	start, pos, end int64
}

// NewRangeReader returns a new RangedReader.
func NewRangeReader(rs io.ReadSeeker, offset, length int64) (io.ReadSeeker, error) {
	_, err := rs.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	return &RangeReader{
		rs:    rs,
		start: offset,
		pos:   offset,
		end:   offset + length,
	}, nil
}

// Read implements io.Read.
func (rr *RangeReader) Read(buf []byte) (int, error) {
	var eof error
	size := int64(len(buf))
	if rr.pos+size >= rr.end {
		size = rr.end - rr.pos
		eof = io.EOF
	}
	n, err := rr.rs.Read(buf[:size])
	if err == nil {
		err = eof
	}
	rr.pos += int64(n)
	return n, err
}

// Seek implements io.Seek.
func (rr *RangeReader) Seek(offset int64, whence int) (int64, error) {
	pos := rr.start
	switch whence {
	case io.SeekStart:
		pos += offset
	case io.SeekCurrent:
		pos = rr.pos + offset
	case io.SeekEnd:
		if offset > 0 {
			offset = 0
		}
		pos = rr.end + offset
	}
	if pos > rr.end {
		rr.pos = rr.end
		return pos, nil
	}
	n, err := rr.rs.Seek(pos, io.SeekStart)
	if err == nil {
		rr.pos = n
	}
	return n - rr.start, err
}
