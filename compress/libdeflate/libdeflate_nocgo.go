// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build !cgo

package libdeflate

// Fall back on the pure-go compress/flate package if cgo support is
// unavailable, to make it safe to include this package unconditionally.

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
)

type Decompressor struct{}

func (dd *Decompressor) Init() error {
	return nil
}

// Decompress performs raw DEFLATE decompression on a byte slice.  outData[]
// must be large enough to fit the decompressed data.  Byte count of the
// decompressed data is returned on success (it may be smaller than
// len(outData)).
func (dd *Decompressor) Decompress(outData, inData []byte) (int, error) {
	dataReader := bytes.NewReader(inData)
	actualDecompressor := flate.NewReader(dataReader)
	// Copy of readToEOF() in github.com/biogo/hts/bgzf/cache.go.
	n := 0
	outDataMax := len(outData)
	var err error
	for err == nil && n < outDataMax {
		var nn int
		nn, err = actualDecompressor.Read(outData[n:])
		n += nn
	}
	switch {
	case err == io.EOF:
		return n, nil
	case n == outDataMax && err == nil:
		var dummy [1]byte
		_, err = actualDecompressor.Read(dummy[:])
		if err == nil {
			return 0, io.ErrShortBuffer
		}
		if err == io.EOF {
			err = nil
		}
	}
	return n, err
}

// GzipDecompress performs gzip decompression on a byte slice.  outData[] must
// be large enough to fit the decompressed data.  Byte count of the
// decompressed data is returned on success (it may be smaller than
// len(outData)).
func (dd *Decompressor) GzipDecompress(outData, inData []byte) (int, error) {
	dataReader := bytes.NewReader(inData)
	actualDecompressor, err := gzip.NewReader(dataReader)
	if err != nil {
		return 0, err
	}
	// Copy of readToEOF() in github.com/biogo/hts/bgzf/cache.go.
	n := 0
	outDataMax := len(outData)
	for err == nil && n < outDataMax {
		var nn int
		nn, err = actualDecompressor.Read(outData[n:])
		n += nn
	}
	switch {
	case err == io.EOF:
		return n, nil
	case n == outDataMax && err == nil:
		var dummy [1]byte
		_, err = actualDecompressor.Read(dummy[:])
		if err == nil {
			return 0, io.ErrShortBuffer
		}
		if err == io.EOF {
			err = nil
		}
	}
	return n, err
}

func (dd *Decompressor) Cleanup() {
}

type Compressor struct {
	clvl int
}

func (cc *Compressor) Init(compressionLevel int) error {
	cc.clvl = compressionLevel
	return nil
}

// Compress performs raw DEFLATE compression on a byte slice.  outData[] must
// be large enough to fit the compressed data.  Byte count of the compressed
// data is returned on success.
// Zero is currently returned on failure.  A side effect is that inData cannot
// be length zero; this function will panic or crash if it is.
func (cc *Compressor) Compress(outData, inData []byte) int {
	// I suspect this currently makes a few unnecessary allocations and copies;
	// can optimize later.
	if len(inData) == 0 {
		panic("libdeflate.Compress: zero-length inData")
	}
	var buf bytes.Buffer
	actualCompressor, err := flate.NewWriter(&buf, cc.clvl)
	if err != nil {
		return 0
	}
	_, err = actualCompressor.Write(inData)
	if err != nil {
		return 0
	}
	err = actualCompressor.Close()
	if err != nil {
		return 0
	}
	outLen := buf.Len()
	if outLen > len(outData) {
		return 0
	}
	copy(outData, buf.Bytes())
	return outLen
}

func (cc *Compressor) Cleanup() {
}
