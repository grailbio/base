// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package fatbin

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/cespare/xxhash"
)

const (
	magic    uint32 = 0x5758ba2c
	headersz        = 20
)

var (
	errNoFooter = errors.New("binary contains no footer")

	bin = binary.LittleEndian
)

func writeFooter(w io.Writer, offset int64) (int, error) {
	var p [headersz]byte
	bin.PutUint64(p[:8], uint64(offset))
	bin.PutUint32(p[8:12], magic)
	bin.PutUint64(p[12:20], xxhash.Sum64(p[:12]))
	return w.Write(p[:])
}

func readFooter(r io.ReaderAt, size int64) (offset int64, err error) {
	if size < headersz {
		return 0, errNoFooter
	}
	var p [headersz]byte
	_, err = r.ReadAt(p[:], size-headersz)
	if err != nil {
		return 0, err
	}
	if bin.Uint32(p[8:12]) != magic {
		return 0, errNoFooter
	}
	offset = int64(bin.Uint64(p[:8]))
	if xxhash.Sum64(p[:12]) != bin.Uint64(p[12:20]) {
		return 0, ErrCorruptedImage
	}
	return
}
