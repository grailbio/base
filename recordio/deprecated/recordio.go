// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package deprecated

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/recordio/internal"
)

// LegacyWriterOpts represents the options accepted by NewLegacyWriter.
type LegacyWriterOpts struct {
	// Marshal is called to marshal an object to a byte slice.
	Marshal MarshalFunc

	// Index is called to enable generating an index for a recordio file; it is
	// called whenever a new record or item is written as per the package
	// level comments.
	Index func(offset, length uint64, v interface{}, p []byte) error
}

// LegacyScannerOpts represents the options accepted by NewScanner.
type LegacyScannerOpts struct {
	// Unmarshal is called to unmarshal an object from the supplied byte slice.
	Unmarshal UnmarshalFunc
}

// LegacyScanner is the interface for reading recordio files as streams of typed
// records. Each record is available as both raw bytes and as type via
// Unmarshal.
type LegacyScanner interface {
	// Reset is equivalent to creating a new scanner, but it retains underlying
	// storage. So it is more efficient than NewScanner. Err is reset to nil. Scan
	// and Bytes will read from rd.
	Reset(rd io.Reader)

	// Scan returns true if a new record was read, false otherwise. It will return
	// false on encoutering an error; the error may be retrieved using the Err
	// method. Note, that Scan will reuse storage from one invocation to the next.
	Scan() bool

	// Bytes returns the current record as read by a prior call to Scan. It may
	// always be called.
	Bytes() []byte

	// Err returns the first error encountered.
	Err() error

	// Unmarshal unmarshals the raw bytes using a preconfigured UnmarshalFunc.
	// It will return an error if there is no preconfigured UnmarshalFunc.
	// Calls to Bytes and Unmarshal may be interspersed.
	Unmarshal(data interface{}) error
}

// Writer is the interface for writing recordio files as streams of typed
// records.
type LegacyWriter interface {
	// Write writes a []byte record to the supplied writer. Each call to write
	// results in a new record being written.
	// Calls to Write and Record may be interspersed.
	io.Writer

	// WriteSlices writes out the supplied slices as a single record, it
	// is intended to avoid having to copy slices into a single slice purely
	// to write them out as a single record.
	WriteSlices(hdr []byte, bufs ...[]byte) (n int, err error)

	// Marshal writes a record using a preconfigured MarshalFunc to the supplied
	// writer. Each call to Record results in a new record being written.
	// Calls to Write and Record may be interspersed.
	Marshal(v interface{}) (n int, err error)
}

const (
	sizeOffset = internal.NumMagicBytes
	crcOffset  = internal.NumMagicBytes + 8
	dataOffset = internal.NumMagicBytes + 8 + crc32.Size
	// teaderSize is the size in bytes of the recordio header.
	headerSize = dataOffset
)

type byteWriter struct {
	wr     io.Writer
	magic  internal.MagicBytes
	hdr    [headerSize]byte
	offset uint64
	opts   LegacyWriterOpts
}

// NewLegacyWriter is DEPRECATED. Use NewWriterV2 instead.
func NewLegacyWriter(wr io.Writer, opts LegacyWriterOpts) LegacyWriter {
	return &byteWriter{
		magic: internal.MagicLegacyUnpacked,
		wr:    wr,
		opts:  opts,
	}
}

func (w *byteWriter) writeHeader(l uint64) (n uint64, err error) {
	marshalHeader(w.hdr[:], w.magic[:], l)
	hdrSize, err := w.wr.Write(w.hdr[:])
	if err != nil {
		return 0, fmt.Errorf("recordio: failed to write header: %v", err)
	}
	return uint64(hdrSize), nil
}

func (w *byteWriter) index(offset, length uint64, v interface{}, p []byte) error {
	if ifn := w.opts.Index; ifn != nil {
		if err := ifn(offset, length, v, p); err != nil {
			return fmt.Errorf("recordio: index callback failed: %v", err)
		}
	}
	return nil
}

func (w *byteWriter) writeBody(p []byte) (n int, err error) {
	n, err = w.wr.Write(p)
	if err != nil {
		return 0, fmt.Errorf("recordio: failed to write record %d bytes: %v", len(p), err)
	}
	w.offset += uint64(n)
	return
}

func (w *byteWriter) Write(p []byte) (n int, err error) {
	hdrSize, err := w.writeHeader(uint64(len(p)))
	if err != nil {
		return 0, err
	}
	if err := w.index(w.offset, uint64(len(p))+hdrSize, nil, nil); err != nil {
		return 0, err
	}
	w.offset += hdrSize
	return w.writeBody(p)
}

// WriteSlices writes the supplied slices as a single record. The arguments
// are specified as a 'first' slice and an arbitrary number of subsequent ones
// to allow for writing a 'header' and 'payload' without forcing the caller
// reallocate and copy their data to match this API. Either first or bufs
// may be nil.
func (w *byteWriter) WriteSlices(first []byte, bufs ...[]byte) (n int, err error) {
	_, _, n, err = w.writeSlices(first, bufs...)
	return
}

func (w *byteWriter) writeSlices(first []byte, bufs ...[]byte) (headerSize, offset uint64, n int, err error) {
	size := uint64(len(first))
	for _, p := range bufs {
		size += uint64(len(p))
	}
	hdrSize, err := w.writeHeader(size)
	if err != nil {
		return 0, 0, 0, err
	}
	offset = w.offset
	if err := w.index(offset, size+hdrSize, nil, nil); err != nil {
		return 0, 0, 0, err
	}
	written := 0
	if len(first) > 0 {
		var err error
		written, err = w.wr.Write(first)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("recordio: failed to write record %d bytes: %v", len(first), err)
		}
	}
	for _, p := range bufs {
		w, err := w.wr.Write(p)
		written += w
		if err != nil {
			return 0, 0, 0, fmt.Errorf("recordio: failed to write record %d bytes: %v", len(p), err)
		}
	}
	w.offset += uint64(written) + hdrSize
	return hdrSize, offset, written, nil
}

// Marshal implements Writer.Marshal.
func (w *byteWriter) Marshal(v interface{}) (n int, err error) {
	mfn := w.opts.Marshal
	if mfn == nil {
		return 0, fmt.Errorf("Marshal function not configured for recordio.Writer")
	}
	p, err := mfn(nil, v)
	if err != nil {
		return 0, err
	}
	hdrSize, err := w.writeHeader(uint64(len(p)))
	if err != nil {
		return 0, err
	}
	if err := w.index(w.offset, uint64(len(p))+hdrSize, v, p); err != nil {
		return 0, err
	}
	w.offset += hdrSize
	return w.writeBody(p)
}

func isErr(err error) bool {
	return err != nil && err != io.EOF
}

// scanner represents scanner for the recordio format.
type LegacyScannerImpl struct {
	rd     io.Reader
	record []byte
	err    errors.Once
	opts   LegacyScannerOpts
	hdr    [headerSize]byte
}

// NewLegacyScanner is DEPRECATED. Use NewScannerV2 instead.
func NewLegacyScanner(rd io.Reader, opts LegacyScannerOpts) LegacyScanner {
	return &LegacyScannerImpl{
		rd:   rd,
		opts: opts,
	}
}

// Reset implements Scanner.Reset.
func (s *LegacyScannerImpl) Reset(rd io.Reader) {
	s.rd = rd
	s.err = errors.Once{}
}

// Unmarshal implements Scanner.Unmarshal.
func (s *LegacyScannerImpl) Unmarshal(v interface{}) error {
	if ufn := s.opts.Unmarshal; ufn != nil {
		return ufn(s.record, v)
	}
	err := fmt.Errorf("Unmarshal function not configured for recordio.Scanner")
	s.err.Set(err)
	return err
}

// Scan implements Scanner.Scan.
func (s *LegacyScannerImpl) Scan() bool {
	magic, ok := s.InternalScan()
	if !ok {
		return false
	}
	if magic != internal.MagicLegacyUnpacked {
		s.err.Set(fmt.Errorf("recordio: invalid magic number: %v, expect %v", magic,
			internal.MagicLegacyUnpacked))
		return false
	}
	return true
}

func (s *LegacyScannerImpl) InternalScan() (internal.MagicBytes, bool) {
	if s.err.Err() != nil {
		return internal.MagicInvalid, false
	}
	n, err := io.ReadFull(s.rd, s.hdr[:])
	if n == 0 && err == io.EOF {
		s.err.Set(io.EOF)
		return internal.MagicInvalid, false
	}
	if isErr(err) {
		s.err.Set(fmt.Errorf("recordio: failed to read header: %v", err))
		return internal.MagicInvalid, false
	}
	magic, size, err := unmarshalHeader(s.hdr[:])
	if err != nil {
		s.err.Set(err)
		return magic, false
	}
	if size == 0 {
		s.record = s.record[:0]
		return magic, true
	}
	if size > internal.MaxReadRecordSize {
		s.record = s.record[:0]
		s.err.Set(fmt.Errorf("recordio: unreasonably large read record encountered: %d > %d bytes", size, internal.MaxReadRecordSize))
		return magic, false
	}
	if uint64(cap(s.record)) < size {
		s.record = make([]byte, size)
	} else {
		s.record = s.record[:size]
	}
	n, err = io.ReadFull(s.rd, s.record)
	if isErr(err) {
		s.err.Set(fmt.Errorf("recordio: failed to read record: %v", err))
		return magic, false
	}
	if uint64(n) != size {
		s.err.Set(fmt.Errorf("recordio: short/long record: %d < %d", n, size))
		return magic, false
	}
	return magic, true
}

// Bytes implements Scanner.Bytes.
func (s *LegacyScannerImpl) Bytes() []byte {
	return s.record
}

// Err implements Scanner.Err.
func (s *LegacyScannerImpl) Err() error {
	err := s.err.Err()
	if err == io.EOF {
		return nil
	}
	return err
}

func marshalHeader(out []byte, magic []byte, size uint64) {
	pos := copy(out, magic)
	binary.LittleEndian.PutUint64(out[pos:], size)
	crc := crc32.Checksum(out[pos:pos+8], internal.IEEECRC)
	pos += 8
	binary.LittleEndian.PutUint32(out[pos:], crc)
}

func unmarshalHeader(buf []byte) (internal.MagicBytes, uint64, error) {
	var magic internal.MagicBytes
	copy(magic[:], buf[0:sizeOffset])
	size := binary.LittleEndian.Uint64(buf[sizeOffset:])
	crc := binary.LittleEndian.Uint32(buf[crcOffset:])
	ncrc := crc32.Checksum(buf[sizeOffset:crcOffset], internal.IEEECRC)
	if ncrc != crc {
		return magic, 0, fmt.Errorf("recordio: crc check failed - corrupt record header (%v != %v)?", ncrc, crc)
	}
	return magic, size, nil
}
