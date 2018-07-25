// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordioutil

import (
	"bytes"
	"io"

	"github.com/klauspost/compress/flate"
)

// FlateTransform represents a 'transform' that can be used with
// recordio.PackedWriter and Scanner to compress/decompress items written to a
// single record.
type FlateTransform struct {
	level       int
	passthrough bool
}

func sizeof(bufs [][]byte) int {
	size := 0
	for _, b := range bufs {
		size += len(b)
	}
	return size
}

// NewFlateTransform creates a new Flate instance for use with
// recordio.PackedWriter and PackedScanner. Level indicates the compression
// level to use as per the flate package's contstants. The chosen level
// has no effect when decompressing.
func NewFlateTransform(level int) *FlateTransform {
	return &FlateTransform{level: level}
}

// CompressTransform is intended for use Recordio.PackedWriterOpts.Transform.
func (f *FlateTransform) CompressTransform(bufs [][]byte) ([]byte, error) {
	if f.passthrough {
		return bytes.Join(bufs, nil), nil
	}
	size := sizeof(bufs)
	out := bytes.NewBuffer(make([]byte, 0, size))
	fwr, err := flate.NewWriter(out, f.level)
	if err != nil {
		return nil, err
	}
	for _, b := range bufs {
		_, err := fwr.Write(b)
		if err != nil {
			return nil, err
		}
	}
	if err := fwr.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// DecompressTransform is intended for use Recordio.PackedScannerOpts.Transform.
func (f *FlateTransform) DecompressTransform(buf []byte) ([]byte, error) {
	if f.passthrough {
		return buf, nil
	}
	// Guess the size of the decompressed buffer.
	size := len(buf) * 2
	out := bytes.NewBuffer(make([]byte, 0, size))
	frd := flate.NewReader(bytes.NewBuffer(buf))
	if _, err := io.Copy(out, frd); err != nil {
		return nil, err
	}
	if err := frd.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
