// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package logio

import (
	"io"

	xxhash "github.com/cespare/xxhash/v2"
)

// Append writes an entry to the io.Writer w. The writer must be
// positioned at the provided offset. If non-nil, Append will use the
// scratch buffer for working space, avoiding additional allocation.
// The scratch buffer must be at least Blocksz.
func Append(w io.Writer, off int64, data, scratch []byte) (nwrite int, err error) {
	if n := off % Blocksz; n > 0 && n < headersz {
		// Corrupted file: skip to the next block boundary.
		//
		// TODO(marius): make sure that in the case of append failure
		// that occurs at the end of the file, that we can recover without
		// exposing the error to the user.
		n, err := w.Write(zeros[:Blocksz-n])
		nwrite += n
		if err != nil {
			return nwrite, err
		}
	} else if left := Blocksz - n; left <= headersz {
		// Need padding.
		n, err := w.Write(zeros[:left])
		nwrite += n
		if err != nil {
			return nwrite, err
		}
	}
	for base := nwrite; len(data) > 0; {
		n := Blocksz - int(off+int64(nwrite))%Blocksz
		n -= headersz
		var typ uint8
		switch {
		case len(data) <= n && nwrite == base:
			typ, n = recordFull, len(data)
		case len(data) <= n:
			typ, n = recordLast, len(data)
		case nwrite == base:
			typ = recordFirst
		default:
			typ = recordMiddle
		}
		scratch = appendRecord(scratch[:0], typ, uint64(nwrite-base), data[:n])
		data = data[n:]
		n, err = w.Write(scratch)
		nwrite += n
		if err != nil {
			return nwrite, err
		}
	}
	return nwrite, nil
}

// Aligned aligns the provided offset for the next write: it returns
// the offset at which the next record will be written, if a writer
// with the provided offset is provided to Append. This can be used
// to index into logio files.
func Aligned(off int64) int64 {
	if n := int64(Blocksz - off%Blocksz); n <= headersz {
		return off + n
	}
	return off
}

// A Writer appends to a log file. Writers are thin stateful wrappers
// around Append.
type Writer struct {
	wr      io.Writer
	off     int64
	scratch []byte
}

// NewWriter returns a new writer that appends log entries to the
// provided io.Writer. The offset given must be the offset into the
// underlying IO stream represented by wr.
func NewWriter(wr io.Writer, offset int64) *Writer {
	return &Writer{wr: wr, off: offset, scratch: make([]byte, Blocksz)}
}

// Append appends a new entry to the log file. Appending an empty
// record is a no-op. Note that the writer appends only appends to
// the underlying stream. It is the responsibility of the caller to
// ensure that the writes are committed to stable storage (e.g., by
// calling file.Sync).
func (w *Writer) Append(data []byte) error {
	n, err := Append(w.wr, w.off, data, w.scratch)
	w.off += int64(n)
	return err
}

// Tell returns the offset of the next record to be appended.
// This may be used to index into the log file.
func (w *Writer) Tell() int64 {
	return Aligned(w.off)
}

// appendRecord appends a record, specified by typ, offset, and data, to p. p
// must have enough capacity for the record.
func appendRecord(p []byte, typ uint8, offset uint64, data []byte) []byte {
	off := len(p)
	p = p[:off+headersz+len(data)]
	p[off+4] = typ
	byteOrder.PutUint16(p[off+5:], uint16(len(data)))
	byteOrder.PutUint64(p[off+7:], offset)
	copy(p[off+15:], data)
	byteOrder.PutUint32(p[off:], checksum(p[off+4:]))
	return p
}

func (w *Writer) write(p []byte) error {
	n, err := w.wr.Write(p)
	w.off += int64(n)
	return err
}

func checksum(data []byte) uint32 {
	h := xxhash.Sum64(data)
	return uint32(h<<32) ^ uint32(h)
}
