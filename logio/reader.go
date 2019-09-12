// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package logio

import (
	"errors"
	"fmt"
	"io"
)

// ErrCorrupted is returned when log file corruption is detected.
var ErrCorrupted = errors.New("corrupted log file")

// Reader reads entries from a log file.
type Reader struct {
	rd  io.Reader
	off int64

	needResync bool

	block block
}

// NewReader returns a log file reader that reads log entries from
// the provider io.Reader. The offset must be the current offset of
// the io.Reader into the IO stream from which records are read.
func NewReader(r io.Reader, offset int64) *Reader {
	return &Reader{rd: r, off: offset}
}

// Read returns the next log entry. It returns ErrCorrupted if a
// corrupted log entry was encountered, in which case the next call
// to Read will re-sync the log file, potentially skipping entries.
// The returned slice should not be modified and is only valid until
// the next call to Read or Rewind.
func (r *Reader) Read() (data []byte, err error) {
	if r.needResync {
		if err := r.resync(); err != nil {
			return nil, err
		}
		r.needResync = false
	}
	for first := true; ; first = false {
		if r.block.eof() {
			err := r.block.read(r.rd, &r.off)
			if err == io.EOF && !first {
				return nil, io.ErrUnexpectedEOF
			} else if err != nil {
				return nil, err
			}
		}
		record, ok := r.block.next()
		switch record.typ {
		case recordFull, recordFirst:
			ok = ok && first
		case recordMiddle, recordLast:
			ok = ok && !first
		}
		if !ok {
			r.needResync = true
			return nil, ErrCorrupted
		}
		switch record.typ {
		case recordFull:
			return record.data, nil
		case recordFirst:
			data = append([]byte{}, record.data...)
		case recordMiddle:
			data = append(data, record.data...)
		case recordLast:
			return append(data, record.data...), nil
		}
	}
}

// Reset resets the reader's state; subsequent entries are
// read from the provided reader at the provided offset.
func (r *Reader) Reset(rd io.Reader, offset int64) {
	*r = Reader{rd: rd, off: offset}
}

func (r *Reader) resync() error {
	for {
		if err := r.block.read(r.rd, &r.off); err != nil {
			return err
		}
		for {
			record, ok := r.block.peek()
			if !ok {
				break
			}
			if record.typ == recordFirst || record.typ == recordFull {
				return nil
			}
			r.block.next()
		}
	}
}

// Rewind finds and returns the offset of the last log entry in the
// log file represented by the reader r. The provided limit is the
// offset of the end of the log stream; thus Rewind may be used to
// traverse a log file in the backwards direction (error handling is
// left as an exercise to the reader):
//
//	file, err := os.Open(...)
//	info, err := file.Stat()
//	off := info.Size()
//	for {
//		off, err = logio.Rewind(file, off)
//		if err == io.EOF {
//			break
//		}
//		file.Seek(off, io.SeekStart)
//		record, err := logio.NewReader(file, off).Read()
// 	}
//
// Rewind returns io.EOF when no records can be located in the
// reader limited by the provided limit.
//
// If the passed reader is also an io.Seeker, then Rewind will seek
// to the returned offset.
func Rewind(r io.ReaderAt, limit int64) (off int64, err error) {
	if s, ok := r.(io.Seeker); ok {
		defer func() {
			if err != nil {
				return
			}
			off, err = s.Seek(off, io.SeekStart)
		}()
	}

	if limit <= headersz {
		return 0, io.EOF
	}
	off = limit - limit%Blocksz
	// Special case: if the limit is on a block boundary, we begin by rewinding
	// to the previous block.
	if off == limit {
		off -= Blocksz
	}
	for ; off >= 0; off -= Blocksz {
		var b block
		off -= off % Blocksz
		if err = b.readLimit(r, off, limit); err != nil {
			return
		}

		// Find the last valid record in the block.
		var last record
		for {
			r, ok := b.next()
			if !ok {
				break
			}
			last = r
		}
		if last.isEmpty() {
			// First record was invalid; try previous block.
			continue
		}

		off += int64(last.blockOff) - int64(last.offset)
		err = b.readLimit(r, off, limit)
		if err != nil {
			return
		}
		if r, ok := b.next(); ok && r.offset == 0 {
			return
		}
	}
	err = io.EOF
	return
}

type record struct {
	blockOff int

	typ    uint8
	offset uint64
	data   []byte
}

func (r record) String() string {
	return fmt.Sprintf("record blockOff:%d typ:%d offset:%d data:%d", r.blockOff, r.typ, r.offset, len(r.data))
}

func (r record) isEmpty() bool {
	return r.blockOff == 0 && r.typ == 0 && r.offset == 0 && r.data == nil
}

type block struct {
	buf        [Blocksz]byte
	off, limit int
	parsed     record
	ok         bool
}

func (b *block) String() string {
	return fmt.Sprintf("block off:%d limit:%d", b.off, b.limit)
}

func (b *block) eof() bool {
	return b.off >= b.limit-headersz && b.parsed.isEmpty()
}

func (b *block) next() (record, bool) {
	rec, ok := b.peek()
	b.parsed = record{}
	return rec, ok
}

func (b *block) peek() (record, bool) {
	if b.parsed.isEmpty() {
		b.parsed, b.ok = b.parse()
	}
	return b.parsed, b.ok
}

func (b *block) parse() (record, bool) {
	if b.off >= b.limit-headersz {
		return record{}, false
	}
	var r record
	r.blockOff = b.off
	chk := b.uint32()
	r.typ = b.uint8()
	length := b.uint16()
	r.offset = b.uint64()
	if int(length) > b.limit-b.off || checksum(b.buf[r.blockOff+4:r.blockOff+headersz+int(length)]) != chk {
		return record{}, false
	}
	r.data = b.bytes(int(length))
	var ok bool
	switch r.typ {
	case recordFirst, recordFull:
		ok = r.offset == 0
	default:
		ok = r.offset != 0
	}
	return r, ok
}

func (b *block) read(r io.Reader, off *int64) error {
	b.reset(Blocksz - int(*off%Blocksz))
	n, err := io.ReadFull(r, b.buf[:b.limit])
	if err == io.ErrUnexpectedEOF {
		b.limit = n
		err = nil
	}
	*off += int64(n)
	return err
}

func (b *block) readLimit(r io.ReaderAt, off, limit int64) error {
	b.reset(Blocksz - int(off%Blocksz))
	if n := limit - off; n < int64(b.limit) {
		b.limit = int(n)
	}
	if b.limit > len(b.buf) {
		panic(off)
	}
	n, err := r.ReadAt(b.buf[:b.limit], off)
	if err == io.EOF && n == b.limit && n < Blocksz {
		err = nil
	}
	return err
}

func (b *block) reset(limit int) {
	b.parsed = record{}
	b.off = 0
	b.limit = limit
}

func (b *block) uint8() uint8 {
	v := b.buf[b.off]
	b.off++
	return uint8(v)
}

func (b *block) uint16() uint16 {
	v := byteOrder.Uint16(b.buf[b.off:])
	b.off += 2
	return v
}

func (b *block) uint32() uint32 {
	v := byteOrder.Uint32(b.buf[b.off:])
	b.off += 4
	return v
}

func (b *block) uint64() uint64 {
	v := byteOrder.Uint64(b.buf[b.off:])
	b.off += 8
	return v
}

func (b *block) bytes(n int) []byte {
	p := b.buf[b.off : b.off+n]
	b.off += n
	return p
}
