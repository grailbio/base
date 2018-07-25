// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package recordioflate provides the "flate" transformer. It implements flate
// compression and decompression.  To use:
//
// - Call recordioflate.Init() when the process starts.
//
// - Add "flate" to WriterV2Opts.Transformer. It will compress blocks using
//   flate default compression level. Setting "flate 3" will enable flate
//   compression level 3.
package recordioflate

import (
	"bytes"
	"io"
	"strconv"
	"sync"

	"github.com/grailbio/base/recordio"
	"github.com/grailbio/base/recordio/recordioiov"
	"github.com/klauspost/compress/flate"
)

// Name is the registered name of the flate transformer.
const Name = "flate"

func flateCompress(level int, bufs [][]byte) ([]byte, error) {
	size := recordioiov.TotalBytes(bufs)
	out := bytes.NewBuffer(make([]byte, 0, size))
	wr, err := flate.NewWriter(out, level)
	if err != nil {
		return nil, err
	}
	for _, b := range bufs {
		n, err := wr.Write(b)
		if err != nil {
			return nil, err
		}
		if n != len(b) {
			panic(b)
		}
	}
	if err := wr.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// FlateUncompress is the uncompress transformer for flate.  This is exposed
// only to read legacy files. For regular applications, adding "flate" to
// ScannerOpts.Transformers will enable flate.
func FlateUncompress(scratch []byte, in [][]byte) ([]byte, error) {
	out := bytes.NewBuffer(scratch[:0])
	r := recordioiov.NewIOVecReader(in)
	frd := flate.NewReader(&r)
	if _, err := io.Copy(out, frd); err != nil {
		return nil, err
	}
	if err := frd.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

var once = sync.Once{}

// Init installs the zstd transformer in recordio.  It can be called multiple
// times, but 2nd and later calls have no effect.
func Init() {
	once.Do(func() {
		recordio.RegisterTransformer(
			Name,
			func(config string) (recordio.TransformFunc, error) {
				level := flate.DefaultCompression
				if config != "" {
					var err error
					level, err = strconv.Atoi(config)
					if err != nil {
						return nil, err
					}
				}
				return func(scratch []byte, in [][]byte) ([]byte, error) {
					// TODO(saito) use scratch
					return flateCompress(level, in)
				}, nil
			},
			func(string) (recordio.TransformFunc, error) {
				return FlateUncompress, nil
			})
	})
}
