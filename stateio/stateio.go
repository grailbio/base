// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package stateio implements persistent state mechanism based on log
// files that interleave indexed state snapshots with state updates. Users maintain
// state by interaction with Reader and Writer objects. A typical application
// should reconcile to the current state before writing new log entries. New
// log entries should be written only after they are known to apply cleanly.
// (In the following examples, error handling is left as an exercise to the
// reader):
//
//	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE)
//	state, epoch, updates, err := stateio.RestoreFile(file)
//	application.Reset(state)
//	for {
//		entry, err := updates.Read()
//		if err == io.EOF {
//			break
//		}
//		application.Update(entry)
//	}
//
//	w, err := stateio.NewFileWriter(file)
//
//	// Apply new state updates:
//	var update []byte = ...
//	application.Update(update)
//	err := w.Update(update)
//
//	// Produce a new snapshot:
//	var snap []byte = application.Snapshot()
//	w.Snapshot(snap)
//
// Data format
//
// State files maintained by package stateio builds on package logio.
// The log file contains a sequence of epochs, each beginning with a
// state snapshot (with the exception of the first epoch, which does
// not need a state snapshot). Each entry is prefixed with the type
// of the entry as well as the the epoch to which the entry belongs.
// Snapshot entries are are prefixed with the previous epoch, so that
// log files can efficiently rewound.
//
// TODO(marius): support log file truncation
package stateio

import "encoding/binary"

const (
	entryUpdate = 1 + iota
	entrySnap

	entryMax
)

func parse(entry []byte) (typ uint8, epoch uint64, data []byte, ok bool) {
	if len(entry) < 9 {
		ok = false
		return
	}
	typ = entry[0]
	epoch = binary.LittleEndian.Uint64(entry[1:])
	data = entry[9:]
	ok = typ < entryMax
	return
}
