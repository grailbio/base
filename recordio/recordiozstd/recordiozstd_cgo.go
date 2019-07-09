// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build cgo

package recordiozstd

import (
	"sync"

	"github.com/DataDog/zstd"
	"github.com/grailbio/base/recordio"
)

func zstdCompress(level int, scratch []byte, in [][]byte) ([]byte, error) {
	if len(in) == 0 {
		return zstd.Compress(scratch, nil)
	}
	if len(in) == 1 {
		return zstd.Compress(scratch, in[0])
	}
	tmp := flattenIov(in)
	d, err := zstd.CompressLevel(scratch, tmp, level)
	tmpBufPool.Put(&tmp)
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
	tmpBufPool.Put(&tmp)
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
				level, err := parseConfig(config)
				if err != nil {
					return nil, err
				}
				if level < 0 {
					level = zstd.DefaultCompression
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
