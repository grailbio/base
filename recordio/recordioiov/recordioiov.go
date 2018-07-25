// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package recordioiov provides utility functions for dealing with [][]bytes, used
// by recordio transformers.
package recordioiov

import (
	"io"
)

// IOVecReader is an io.Reader adapter for [][]byte.
type IOVecReader struct {
	in       [][]byte
	curIndex int
	curBuf   []byte
}

// NewIOVecReader creates a new io.Reader that reads from the given buf.
func NewIOVecReader(buf [][]byte) IOVecReader {
	return IOVecReader{buf, -1, nil}
}

func (r *IOVecReader) fillBuf() error {
	for len(r.curBuf) == 0 {
		r.curIndex++
		if r.curIndex >= len(r.in) {
			return io.EOF
		}
		r.curBuf = r.in[r.curIndex]
	}
	return nil
}

// Read implements the io.Reader interface.
func (r *IOVecReader) Read(p []byte) (int, error) {
	if err := r.fillBuf(); err != nil {
		return 0, err
	}
	bytes := len(p)
	if bytes > len(r.curBuf) {
		bytes = len(r.curBuf)
	}
	copy(p, r.curBuf)
	r.curBuf = r.curBuf[bytes:]
	return bytes, nil
}

// ReadByte reads one byte from the reader. On EOF, returns (0, io.EOF).
func (r *IOVecReader) ReadByte() (byte, error) {
	if err := r.fillBuf(); err != nil {
		return 0, err
	}
	b := r.curBuf[0]
	r.curBuf = r.curBuf[1:]
	return b, nil
}

// TotalBytes returns the total size of the iovec.
func TotalBytes(buf [][]byte) int {
	size := 0
	for _, b := range buf {
		size += len(b)
	}
	return size
}

// Slice returns a byte slice of the given size. If cap(buf)>=size, then it
// returns buf[:size]. Else it allocates a new slice on heap.
func Slice(buf []byte, size int) []byte {
	if cap(buf) >= size {
		return buf[:size]
	}
	return make([]byte, size)
}
