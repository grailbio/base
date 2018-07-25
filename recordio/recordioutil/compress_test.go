// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordioutil_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/grailbio/base/recordio/recordioutil"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/expect"
	"github.com/klauspost/compress/flate"
)

func transformTestData(n int) [][]byte {
	r := [][]byte{}
	for i := 0; i < n; i++ {
		r = append(r, []byte(strings.Repeat(fmt.Sprintf("%v", i), i)))
	}
	return r
}

func TestCompressInverse(t *testing.T) {
	data := transformTestData(10)

	ft := recordioutil.NewFlateTransform(flate.BestCompression)

	record, err := ft.CompressTransform(data)
	if err != nil {
		t.Fatal(err)
	}

	read, err := ft.DecompressTransform(record)
	assert.NoError(t, err)
	assert.EQ(t, read, bytes.Join(data, nil))
}

func TestCompressErrors(t *testing.T) {
	data := transformTestData(1)
	ft := recordioutil.NewFlateTransform(33)
	_, err := ft.CompressTransform(data)
	expect.HasSubstr(t, err, "invalid compression level")
}
