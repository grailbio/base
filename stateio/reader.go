// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package stateio

import (
	"errors"
	"io"
	"os"

	"github.com/grailbio/base/logio"
)

// ErrCorrupt is returned when a corrupted log is encountered.
var ErrCorrupt = errors.New("corrupt state entry")

// Restore restores the state from the last epoch in the state log
// read by the provided reader and the given limit. The returned
// state may be nil if no snapshot was defined for the epoch.
func Restore(r io.ReaderAt, limit int64) (state []byte, epoch uint64, updates *Reader, err error) {
	if limit == 0 {
		return nil, 0, nil, nil
	}
	off, err := logio.Rewind(r, limit)
	if err != nil {
		return
	}
	reader := &readerAtReader{r, off}
	log := logio.NewReader(reader, off)
	entry, err := log.Read()
	if err != nil {
		return
	}
	var (
		typ  uint8
		data []byte
		ok   bool
	)
	typ, epoch, data, ok = parse(entry)
	if !ok {
		// TODO(marius): let the user deal with this? perhaps by providing
		// a utility function in package logio to skip corrupted entries.
		err = ErrCorrupt
		return
	}
	if typ == entrySnap {
		// Special case: the first entry is a snapshot, so we need to restore
		// the correct epoch.
		epoch = uint64(off)
	} else {
		reader.off = int64(epoch)
		log.Reset(reader, reader.off)
		entry, err = log.Read()
		if err != nil {
			return
		}
		typ, _, data, ok = parse(entry)
		if !ok {
			err = ErrCorrupt
			return
		}
	}

	if typ == entrySnap {
		state = append([]byte{}, data...)
	} else {
		reader.off = int64(epoch)
		log.Reset(reader, reader.off)
	}
	updates = &Reader{log, epoch}
	return
}

// RestoreFile is a convenience function that restores the file from
// the provided os file.
func RestoreFile(file *os.File) (state []byte, epoch uint64, updates *Reader, err error) {
	off, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, 0, nil, err
	}
	state, epoch, updates, err = Restore(file, off)
	if _, e := file.Seek(off, io.SeekStart); e != nil && err == nil {
		err = e
	}
	return
}

// Reader reads a single epoch state updates.
type Reader struct {
	log    *logio.Reader
	offset uint64
}

// Read returns the next state update entry. Read returns ErrCorrupt
// if a corrupted log entry was encountered, or logio.ErrCorrupt is a
// corrupt log file was encountered. In the latter case, the user may
// skip the corrupted entry by issuing another read.
func (r *Reader) Read() ([]byte, error) {
	if r == nil {
		return nil, io.EOF
	}
	entry, err := r.log.Read()
	if err != nil {
		return nil, err
	}
	typ, offset, data, ok := parse(entry)
	if !ok {
		return nil, ErrCorrupt
	}
	if typ == entrySnap {
		return nil, io.EOF
	}
	if offset != r.offset {
		// We should always encounter a new snapshot before an offset change.
		return nil, ErrCorrupt
	}
	return data, nil
}

type readerAtReader struct {
	r   io.ReaderAt
	off int64
}

func (r *readerAtReader) Read(p []byte) (n int, err error) {
	n, err = r.r.ReadAt(p, r.off)
	if err == io.ErrUnexpectedEOF {
		err = nil
	}
	r.off += int64(n)
	return n, err
}
