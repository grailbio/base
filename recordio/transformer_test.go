// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordio_test

import (
	"bytes"
	"testing"

	"github.com/grailbio/base/recordio"
	"github.com/grailbio/base/recordio/recordioflate"
	"github.com/grailbio/base/recordio/recordiozstd"
	"github.com/grailbio/testutil/assert"
)

// Produce a recordio using transformer "name". Returns the ratio between the
// encoded size and input size. For a compressing transformer, the ratio should
// be ⋘ 1.
func transformerTest(t *testing.T, name string) float64 {
	buf := &bytes.Buffer{}
	wr := recordio.NewWriter(buf, recordio.WriterOpts{
		Transformers: []string{name},
	})
	// Write lots of compressible data.
	const itemSize = 16 << 8
	const nRecs = 300
	for i := 0; i < nRecs; i++ {
		data := make([]byte, itemSize)
		for j := range data {
			data[j] = 'A' + byte(i)
		}
		wr.Append(data)
	}
	assert.NoError(t, wr.Finish())

	// Verify the data
	sc := recordio.NewScanner(bytes.NewReader(buf.Bytes()), recordio.ScannerOpts{})
	for i := 0; i < nRecs; i++ {
		assert.True(t, sc.Scan(), "err: %v", sc.Err())
		data := sc.Get().([]byte)
		assert.EQ(t, len(data), itemSize)
		for j := range data {
			assert.EQ(t, data[j], byte('A'+i))
		}
	}
	assert.False(t, sc.Scan())
	assert.NoError(t, sc.Err())
	return float64(len(buf.Bytes())) / float64(nRecs*itemSize)
}

func TestZstd(t *testing.T) {
	recordiozstd.Init()
	ratio := transformerTest(t, recordiozstd.Name)
	assert.LT(t, ratio, 0.2)
}

func TestFlate(t *testing.T) {
	recordioflate.Init()
	ratio := transformerTest(t, recordioflate.Name)
	assert.LT(t, ratio, 0.2)
}
