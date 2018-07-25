// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordioutil_test

import (
	"bytes"
	"testing"

	"github.com/grailbio/base/fileio"
	"github.com/grailbio/base/recordio"
	"github.com/grailbio/base/recordio/recordioutil"
	"github.com/grailbio/base/recordio/deprecated"
	"github.com/stretchr/testify/require"
)

func readV1(t *testing.T, format fileio.FileType, buf *bytes.Buffer) (s []string) {
	opts, err := recordioutil.ScannerOptsFromName("foo." + fileio.FileSuffix(format))
	if err != nil {
		s = append(s, err.Error())
	}
	sc := recordio.NewScanner(bytes.NewReader(buf.Bytes()), opts)
	for sc.Scan() {
		s = append(s, string(sc.Get().([]byte)))
	}
	if err := sc.Err(); err != nil {
		s = append(s, err.Error())
	}
	return
}

func TestPacked(t *testing.T) {
	buf := &bytes.Buffer{}
	w := deprecated.NewLegacyPackedWriter(buf, deprecated.LegacyPackedWriterOpts{})
	w.Write([]byte("Foo"))
	w.Write([]byte("Baz"))
	w.Flush()
	require.Equal(t, []string{"Foo", "Baz"}, readV1(t, fileio.GrailRIOPacked, buf))
}

func TestUnpacked(t *testing.T) {
	buf := &bytes.Buffer{}
	w := deprecated.NewLegacyWriter(buf, deprecated.LegacyWriterOpts{})
	w.Write([]byte("Foo"))
	w.Write([]byte("Baz"))
	require.Equal(t, []string{"Foo", "Baz"}, readV1(t, fileio.GrailRIO, buf))
}

func TestCompressed(t *testing.T) {
	buf := &bytes.Buffer{}
	w := deprecated.NewLegacyPackedWriter(buf, deprecated.LegacyPackedWriterOpts{
		Transform: recordioutil.NewFlateTransform(-1).CompressTransform})
	w.Write([]byte("Foo"))
	w.Write([]byte("Baz"))
	w.Flush()
	require.Equal(t, []string{"Foo", "Baz"}, readV1(t, fileio.GrailRIOPackedCompressed, buf))
}
