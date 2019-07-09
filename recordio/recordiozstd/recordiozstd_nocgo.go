// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build !cgo

package recordiozstd

import (
	"sync"

	"github.com/grailbio/base/recordio"
	"github.com/klauspost/compress/zstd"
)

var once sync.Once

// Init registers a dummy implementation for recordio zstd compression.
// The registered transformers always return an error.
func Init() {
	once.Do(func() {
		recordio.RegisterTransformer(
			Name,
			func(config string) (recordio.TransformFunc, error) {
				l, err := parseConfig(config)
				if err != nil {
					return nil, err
				}
				level := zstd.EncoderLevel(l)
				if level < 0 {
					level = zstd.SpeedDefault
				}
				return func(scratch []byte, in [][]byte) ([]byte, error) {
					// TODO(saito) reuse the writer using sync.Pool
					w, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
					if err != nil {
						return nil, err
					}
					tmp := flattenIov(in)
					dst := w.EncodeAll(tmp, scratch[:0])
					tmpBufPool.Put(&tmp)
					return dst, nil
				}, nil
			},
			func(string) (recordio.TransformFunc, error) {
				return func(scratch []byte, in [][]byte) ([]byte, error) {
					// TODO(saito) reuse the reader using sync.Pool
					r, err := zstd.NewReader(nil)
					if len(in) == 0 {
						return r.DecodeAll(nil, scratch[:0])
					}
					if len(in) == 1 {
						return r.DecodeAll(in[0], scratch[:0])
					}
					tmp := flattenIov(in)
					d, err := r.DecodeAll(tmp, scratch[:0])
					tmpBufPool.Put(&tmp)
					return d, err
				}, nil
			})
	})
}
