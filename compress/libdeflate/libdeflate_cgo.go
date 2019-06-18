// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build cgo

package libdeflate

/*
#include "libdeflate.h"
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// Decompressor is a minimal interface to libdeflate's decompressor object.  It
// allocates a C memory area, so users are responsible for calling
// Decompressor.Cleanup() when done to avoid memory leaks.  Multiple
// Decompressors can be active at once.
//
// The Reader interface is not directly implemented here; instead, it is
// assumed that the user is decompressing a multi-block BGZF-like format, and
// they're fine with manual calls to Decompress().
type Decompressor struct {
	cobj *C.struct_libdeflate_decompressor
}

// Init allocates workspace needed by Decompress().
func (dd *Decompressor) Init() error {
	if dd.cobj == nil {
		dd.cobj = C.libdeflate_alloc_decompressor()
		if dd.cobj == nil {
			return fmt.Errorf("libdeflate: failed to allocate decompressor")
		}
	}
	return nil
}

// Decompress performs raw DEFLATE decompression on a byte slice.  outData[]
// must be large enough to fit the decompressed data.  Byte count of the
// decompressed data is returned on success (it may be smaller than
// len(outData)).
func (dd *Decompressor) Decompress(outData, inData []byte) (int, error) {
	// Tolerate zero-length blocks on input, even though we don't on output.
	// (Can be relevant when a BGZF file was formed by raw byte concatenation of
	// smaller BGZF files.)
	// Note that we can't use the usual *reflect.SliceHeader replacement for
	// unsafe.Pointer(&inData[0]): that produces a "cgo argument has Go pointer
	// to Go pointer" compile error.
	if len(inData) == 0 {
		return 0, nil
	}
	var outLen C.size_t
	errcode := C.libdeflate_deflate_decompress(
		dd.cobj, unsafe.Pointer(&inData[0]), C.size_t(len(inData)),
		unsafe.Pointer(&outData[0]), C.size_t(len(outData)), &outLen)
	if errcode != C.LIBDEFLATE_SUCCESS {
		return 0, fmt.Errorf("libdeflate: libdeflate_deflate_decompress() error code %d", errcode)
	}
	return int(outLen), nil
}

// GzipDecompress performs gzip decompression on a byte slice.  outData[] must
// be large enough to fit the decompressed data.  Byte count of the
// decompressed data is returned on success (it may be smaller than
// len(outData)).
func (dd *Decompressor) GzipDecompress(outData, inData []byte) (int, error) {
	if len(inData) == 0 {
		return 0, nil
	}
	var outLen C.size_t
	errcode := C.libdeflate_gzip_decompress(
		dd.cobj, unsafe.Pointer(&inData[0]), C.size_t(len(inData)),
		unsafe.Pointer(&outData[0]), C.size_t(len(outData)), &outLen)
	if errcode != C.LIBDEFLATE_SUCCESS {
		return 0, fmt.Errorf("libdeflate: libdeflate_gzip_decompress() error code %d", errcode)
	}
	return int(outLen), nil
}

// Cleanup frees workspace memory.
func (dd *Decompressor) Cleanup() {
	if dd.cobj != nil {
		C.libdeflate_free_decompressor(dd.cobj)
		dd.cobj = nil
	}
}

// Compressor is a minimal interface to libdeflate's compressor object.
type Compressor struct {
	cobj *C.struct_libdeflate_compressor
}

// Init allocates workspace needed by Compress().
func (cc *Compressor) Init(compressionLevel int) error {
	if cc.cobj == nil {
		if compressionLevel == -1 {
			compressionLevel = 5
		}
		cc.cobj = C.libdeflate_alloc_compressor(C.int(compressionLevel))
		if cc.cobj == nil {
			return fmt.Errorf("libdeflate: failed to allocate compressor")
		}
	}
	return nil
}

// Compress performs raw DEFLATE compression on a byte slice.  outData[] must
// be large enough to fit the compressed data.  Byte count of the compressed
// data is returned on success.
// Zero is currently returned on failure.  A side effect is that inData cannot
// be length zero; this function will panic or crash if it is.
func (cc *Compressor) Compress(outData, inData []byte) int {
	// We *want* to crash on length-zero (that implies an error in the calling
	// code, we don't want to be writing zero-length BGZF blocks without knowing
	// about it), so we intentionally exclude the len(inData) == 0 check.
	outLen := int(C.libdeflate_deflate_compress(
		cc.cobj, unsafe.Pointer(&inData[0]), C.size_t(len(inData)),
		unsafe.Pointer(&outData[0]), C.size_t(len(outData))))
	return outLen
}

// Cleanup frees workspace memory.
func (cc *Compressor) Cleanup() {
	if cc.cobj != nil {
		C.libdeflate_free_compressor(cc.cobj)
		cc.cobj = nil
	}
}
