// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package libdeflate

import (
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/grailbio/base/unsafe"
)

// This is a slightly modified version of klauspost/compress/gzip/gzip.go , and
// is designed to be a drop-in replacement for it in bgzf writer code.  It may
// eventually go into its own sub-package, but for now I'll keep it here since
// bgzf is our only use case (zstd is generally a better choice for new custom
// formats).

// These constants are copied from klauspost/compress/gzip.  The
// BestCompression value is technically a lie for libdeflate--it goes all the
// way up to 12, not 9--so I've defined an extra constant for the real highest
// setting.
const (
	BestSpeed          = gzip.BestSpeed
	BestCompression    = gzip.BestCompression
	BestestCompression = 12
	DefaultCompression = gzip.DefaultCompression

	// NoCompression and ConstantCompression/HuffmanOnly are not supported by
	// this package.

	DefaultBufCap = 0x10000

	gzipID1     = 0x1f
	gzipID2     = 0x8b
	gzipDeflate = 8
)

// A Writer is an io.WriteCloser.
// Writes to a Writer are compressed and written to w.
type Writer struct {
	gzip.Header
	w          io.Writer
	level      int
	compressor Compressor
	digest     uint32 // CRC-32, IEEE polynomial (section 8)
	size       uint32 // Uncompressed size (section 2.3.1)
	closed     bool
	buf        []byte

	// Next two fields are specific to libdeflate.
	// NewWriter{Level}() can't accept a bufCap argument without breaking
	// klauspost/compress/gzip compatibility, so we have bufCap default to the
	// 65536 value needed by bgzf, export a function that changes it, and
	// lazy-initialize buf[] so that we don't allocate a size-65536 block only to
	// immediately throw it away if the user wants a different capacity.
	// bufCap must be large enough to fit the entire gzip block, not just the
	// compressed portion.
	bufCap int
	bufPos int

	// wroteHeader has been removed since it's equivalent to (bufPos != 0).

	err error
}

// NewWriter returns a new Writer.
// Writes to the returned writer are compressed and written to w.
//
// It is the caller's responsibility to call Close on the WriteCloser when
// done.
// Writes may be buffered and not flushed until Close.
//
// Callers that wish to set the fields in Writer.Header must do so before the
// first call to Write, Flush, or Close.
func NewWriter(w io.Writer) *Writer {
	z, _ := NewWriterLevel(w, DefaultCompression)
	return z
}

// NewWriterLevel is like NewWriter but specifies the compression level instead
// of assuming DefaultCompression.
//
// The compression level can be DefaultCompression, or any integer value
// between 1 and BestCompression inclusive.  The error returned will be nil if
// the level is valid.
func NewWriterLevel(w io.Writer, level int) (*Writer, error) {
	if (level < DefaultCompression) || (level == 0) || (level > BestestCompression) {
		return nil, fmt.Errorf("libdeflate: invalid compression level: %d", level)
	}
	z := new(Writer)

	z.bufCap = DefaultBufCap

	z.init(w, level)
	return z, nil
}

func (z *Writer) init(w io.Writer, level int) {
	// compressor is now lazy-(re)initialized later.

	buf := z.buf
	bufCap := z.bufCap

	*z = Writer{
		Header: gzip.Header{
			OS: 255, // unknown
		},
		w:      w,
		level:  level,
		buf:    buf,
		bufCap: bufCap,
	}
}

// Reset discards the Writer z's state and makes it equivalent to the result of
// its original state from NewWriter or NewWriterLevel, but writing to w
// instead.  This permits reusing a Writer rather than allocating a new one.
//
// It is safe to call Reset() without Close().  In this case, *no* bytes from
// the previous block are written.
func (z *Writer) Reset(w io.Writer) {
	z.init(w, z.level)
}

// SetCap changes the capacity of the final write buffer, which must fit the
// entire gzip block (header and footer bytes included).  (libdeflate requires
// this value to be declared in advance.)
// If this function isn't called, the capacity is set to DefaultBufCap ==
// 0x10000.
func (z *Writer) SetCap(newCap int) error {
	if z.bufPos != 0 {
		return errors.New("libdeflate.SetCap: invalid call (must immediately follow initialization/reset)")
	}
	if newCap < 18 {
		// guarantee enough space for always-present header and footer bytes, so we
		// can be slightly less paranoid with bounds-checks
		return errors.New("libdeflate.SetCap: capacity too low")
	}
	z.bufCap = newCap
	return nil
}

var le = binary.LittleEndian

// appendBytes appends a length-prefixed byte slice to z.buf.
func (z *Writer) appendBytes(b []byte) error {
	if len(b) > 0xffff {
		return errors.New("libdeflate.Write: extra data is too large")
	}
	midPos := z.bufPos + 2
	endPos := midPos + len(b)
	if endPos > z.bufCap {
		return errors.New("libdeflate.Write: out of buffer space")
	}
	le.PutUint16(z.buf[z.bufPos:], uint16(len(b)))
	copy(z.buf[midPos:], b)
	z.bufPos = endPos
	return nil
}

// appendString appends a UTF-8 string s in GZIP's format to z.buf.
// GZIP (RFC 1952) specifies that strings are NUL-terminated ISO 8859-1
// (Latin-1).
func (z *Writer) appendString(s string) (err error) {
	// GZIP stores Latin-1 strings; error if non-Latin-1; convert if non-ASCII.
	needconv := false
	for _, v := range s {
		if v == 0 || v > 0xff {
			return errors.New("libdeflate.Write: non-Latin-1 header string")
		}
		if v > 0x7f {
			needconv = true
		}
	}
	nulPos := z.bufPos
	if needconv {
		b := make([]byte, 0, len(s))
		for _, v := range s {
			b = append(b, byte(v))
		}
		nulPos += len(b)
		if nulPos >= z.bufCap {
			return errors.New("libdeflate.Write: out of buffer space")
		}
		copy(z.buf[z.bufPos:], b)
	} else {
		nulPos += len(s)
		if nulPos >= z.bufCap {
			return errors.New("libdeflate.Write: out of buffer space")
		}
		copy(z.buf[z.bufPos:], s)
	}
	// GZIP strings are NUL-terminated.
	z.buf[nulPos] = 0
	z.bufPos = nulPos + 1
	return nil
}

// Write writes a compressed form of p to the underlying io.Writer. The
// compressed bytes are not necessarily flushed until the Writer is closed.
func (z *Writer) Write(p []byte) (int, error) {
	if z.err != nil {
		return 0, z.err
	}
	// Enforce the libdeflate constraint.
	if z.bufPos != 0 {
		z.err = errors.New("libdeflate.Write: only one Write operation permitted per block")
		return 0, z.err
	}
	// 'Write' the GZIP header lazily.
	if len(z.buf) < z.bufCap {
		if cap(z.buf) < z.bufCap {
			// Impossible to avoid zero-initialization here in Go.
			z.buf = make([]byte, z.bufCap)
		} else {
			// No need to zero-reinitialize.
			unsafe.ExtendBytes(&z.buf, z.bufCap)
		}
	} else if len(z.buf) > z.bufCap {
		// Likely to be irrelevant, but may as well maintain this invariant
		z.buf = z.buf[:z.bufCap]
	}
	z.buf[0] = gzipID1
	z.buf[1] = gzipID2
	z.buf[2] = gzipDeflate
	z.buf[3] = 0
	if z.Extra != nil {
		z.buf[3] |= 0x04
	}
	if z.Name != "" {
		z.buf[3] |= 0x08
	}
	if z.Comment != "" {
		z.buf[3] |= 0x10
	}
	le.PutUint32(z.buf[4:8], uint32(z.ModTime.Unix()))
	if z.level >= BestCompression {
		// Reasonable to set this for any level in 9..12.
		z.buf[8] = 2
	} else if z.level == BestSpeed {
		z.buf[8] = 4
	} else {
		z.buf[8] = 0
	}
	z.buf[9] = z.OS
	z.bufPos = 10
	if z.Extra != nil {
		z.err = z.appendBytes(z.Extra)
		if z.err != nil {
			return 0, z.err
		}
	}
	if z.Name != "" {
		z.err = z.appendString(z.Name)
		if z.err != nil {
			return 0, z.err
		}
	}
	if z.Comment != "" {
		z.err = z.appendString(z.Comment)
		if z.err != nil {
			return 0, z.err
		}
	}
	z.err = z.compressor.Init(z.level)
	if z.err != nil {
		return 0, z.err
	}
	z.size += uint32(len(p))
	z.digest = crc32.Update(z.digest, crc32.IEEETable, p)

	n := z.compressor.Compress(z.buf[z.bufPos:z.bufCap-8], p)
	z.bufPos += n
	if n == 0 {
		z.err = errors.New("libdeflate.Write: out of buffer space")
	}
	return n, z.err
}

// Flush() has been removed for now.

// Close closes the Writer, flushing any unwritten data to the underlying
// io.Writer, but does not close the underlying io.Writer.
func (z *Writer) Close() error {
	if z.err != nil {
		return z.err
	}
	if z.closed {
		return nil
	}
	z.closed = true
	if z.bufPos == 0 {
		_, z.err = z.Write(nil)
		if z.err != nil {
			return z.err
		}
	}
	// a bit inefficient to keep calling this, but given the current interface we
	// have no choice.
	z.compressor.Cleanup()
	midPos := z.bufPos + 4
	endPos := midPos + 4
	if endPos > z.bufCap {
		z.err = errors.New("libdeflate.Write: out of buffer space")
	}
	le.PutUint32(z.buf[z.bufPos:], z.digest)
	le.PutUint32(z.buf[midPos:], z.size)
	_, z.err = z.w.Write(z.buf[:endPos])
	return z.err
}
