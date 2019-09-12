// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package logio implements a failure-tolerant log, typically used as
// a write-ahead log. Logs are "history oblivious": new log entries
// do not depend on previous entries; and logs may be concatenated on
// block boundaries while preserving integrity. Likewise, logs may be
// read from a stream without seeking.
//
// Data layout
//
// Logio follows the leveldb log format [1] with some modifications
// to permit efficient re-syncing from the end of a log, as well as
// to use a modern checksum algorithm (xxhash).
//
// A log file is a sequence of 32kB blocks, each containing a sequence
// of records and possibly followed by padding. Records may not span
// blocks; log entries that would straddle block boundaries are broken
// up into multiple records, to be reassembled at read time.
//
//	block := record* padding?
//
//	record :=
//		checksum uint32     // xxhash[2] checksum of the remainder of the record
//		type uint8          // the record type, detailed below
//		length uint16       // the length of the record data, below
//		offset uint64       // the offset (in bytes) of this record from the record that begins the entry
//		data [length]uint8  // the record data
//
// The record types are as follows:
//
//	FULL=1     // the record contains the full entry
//	FIRST=2    // the record is the first in an assembly
//	MIDDLE=3   // the record is in the middle of an assembly
//	LAST=4     // the record concludes an assembly
//
// Thus, entries are assembled by reading a sequence of records:
//
//	entry :=
//		  FULL
//		| FIRST MIDDLE* LAST
//
// Failure tolerance
//
// Logio recovers from record corruption (e.g., checksum errors) and truncated
// writes by re-syncing at read time. If a corrupt record is encountered, the
// reader skips to the next block boundary (which always begins a record) and
// finds the first FULL or FIRST record to re-commence reading.
//
// [1] https://github.com/google/leveldb/blob/master/doc/log_format.md
// [2] http://cyan4973.github.io/xxHash/
package logio

import (
	"encoding/binary"
)

// Blocksz is the size of the blocks written to the log files
// produced by this package. See package docs for a detailed
// description.
const Blocksz = 32 << 10

const headersz = 4 + 1 + 2 + 8

var byteOrder = binary.LittleEndian

var zeros = make([]byte, Blocksz)

const (
	recordFull uint8 = 1 + iota
	recordFirst
	recordMiddle
	recordLast
)
