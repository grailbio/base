// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package recordiozstd

import (
	"strconv"
	"sync"

	"github.com/grailbio/base/recordio/recordioiov"
)

// Name is the registered name of the zstd transformer.
const Name = "zstd"

func parseConfig(config string) (level int, err error) {
	level = -1
	if config != "" {
		level, err = strconv.Atoi(config)
	}
	return
}

var tmpBufPool = sync.Pool{New: func() interface{} { return &[]byte{} }}

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

	// storing only pointers in sync.Pool per https://github.com/golang/go/issues/16323
	slicePtr := tmpBufPool.Get().(*[]byte)
	tmp := recordioiov.Slice(*slicePtr, totalBytes)
	n := 0
	for _, inbuf := range in {
		copy(tmp[n:], inbuf)
		n += len(inbuf)
	}
	return tmp
}
