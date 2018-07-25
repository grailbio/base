// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build cgo

// Package recordiozstd implements zstd compression and decompression.
//
// Adding "zstd" to WriterV2Opts.Transformer enables zstd with default
// compression level.  "zstd 6" will enable zstd with compression level 6.
package recordiozstd

import (
	"strconv"
	"sync"

	"github.com/DataDog/zstd"
	"github.com/grailbio/base/recordio"
	"github.com/grailbio/base/recordio/recordioiov"
)

var tmpBufPool = sync.Pool{New: func() interface{} { return []byte{} }}

// As of 2018-03, zstd.{Compress,Decompress} is much faster than
// io.{Reader,Writer}-based implementations, even though the former incurs extra
// copying.
//
// Reader/Writer impl:
// BenchmarkWrite-56             20         116151712 ns/op
// BenchmarkRead-56              30          45302918 ns/op
//
// Compress/Decompress impl:
// BenchmarkWrite-56    	      50	  30034396 ns/op
// BenchmarkRead-56    	      50	  23871334 ns/op
func flattenIov(in [][]byte) []byte {
	totalBytes := recordioiov.TotalBytes(in)
	tmp := recordioiov.Slice(tmpBufPool.Get().([]byte), totalBytes)
	n := 0
	for _, inbuf := range in {
		copy(tmp[n:], inbuf)
		n += len(inbuf)
	}
	return tmp
}

func zstdCompress(level int, scratch []byte, in [][]byte) ([]byte, error) {
	if len(in) == 0 {
		return zstd.Compress(scratch, nil)
	}
	if len(in) == 1 {
		return zstd.Compress(scratch, in[0])
	}
	tmp := flattenIov(in)
	d, err := zstd.CompressLevel(scratch, tmp, level)
	tmpBufPool.Put(tmp)
	return d, err
}

func zstdUncompress(scratch []byte, in [][]byte) ([]byte, error) {
	if len(in) == 0 {
		return zstd.Decompress(scratch, nil)
	}
	if len(in) == 1 {
		return zstd.Decompress(scratch, in[0])
	}
	tmp := flattenIov(in)
	d, err := zstd.Decompress(scratch, tmp)
	tmpBufPool.Put(tmp)
	return d, err
}

var once = sync.Once{}

// Init installs the zstd transformer in recordio.  It can be called multiple
// times, but 2nd and later calls have no effect.
func Init() {
	once.Do(func() {
		recordio.RegisterTransformer(
			Name,
			func(config string) (recordio.TransformFunc, error) {
				level := zstd.DefaultCompression
				if config != "" {
					var err error
					level, err = strconv.Atoi(config)
					if err != nil {
						return nil, err
					}
				}
				return func(scratch []byte, in [][]byte) ([]byte, error) {
					return zstdCompress(level, scratch, in)
				}, nil
			},
			func(string) (recordio.TransformFunc, error) {
				return zstdUncompress, nil
			})
	})
}
