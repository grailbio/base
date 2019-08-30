// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package stateio

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/grailbio/base/logio"
)

type syncer interface {
	Sync() error
}

// Writer writes snapshots and update entries to an underlying log stream.
type Writer struct {
	syncer syncer
	log    *logio.Writer
	epoch  uint64
}

// NewWriter initializes and returns a new state log writer which
// writes to the stream w, which is positioned at the provided
// offset. The provided epoch must be the current epoch of the log
// file. If the provided io.Writer is also a syncer:
//
//	type Syncer interface {
//		Sync() error
//	}
//
// Then Sync() is called (and errors returned) after each log entry
// has been written.
func NewWriter(w io.Writer, off int64, epoch uint64) *Writer {
	wr := &Writer{log: logio.NewWriter(w, off), epoch: epoch}
	if s, ok := w.(syncer); ok {
		wr.syncer = s
	}
	return wr
}

// NewFileWriter initializes a state log writer from the provided
// os file.  The file's contents is committed to stable storage
// after each log write.
func NewFileWriter(file *os.File) (*Writer, error) {
	off, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	_, epoch, _, err := Restore(file, off)
	if err != nil {
		return nil, err
	}
	off, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	return NewWriter(file, off, epoch), nil
}

// Snapshot writes a new snapshot to the state log.
// Subsequent updates are based on this snapshot.
func (w *Writer) Snapshot(snap []byte) error {
	off := w.log.Tell()
	entry := make([]byte, len(snap)+9)
	entry[0] = entrySnap
	binary.LittleEndian.PutUint64(entry[1:], w.epoch)
	copy(entry[9:], snap)
	if err := w.log.Append(entry); err != nil {
		return err
	}
	w.epoch = uint64(off)
	return w.sync()
}

// Update writes a new state update to the log. The update
// refers to the last snapshot written.
func (w *Writer) Update(update []byte) error {
	entry := make([]byte, 9+len(update))
	entry[0] = entryUpdate
	binary.LittleEndian.PutUint64(entry[1:], uint64(w.epoch))
	copy(entry[9:], update)
	if err := w.log.Append(entry); err != nil {
		return err
	}
	return w.sync()
}

func (w *Writer) sync() error {
	if w.syncer == nil {
		return nil
	}
	return w.syncer.Sync()
}
