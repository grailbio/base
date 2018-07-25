// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package deprecated_test

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/grailbio/base/recordio/deprecated"
	"github.com/grailbio/testutil"
)

func TestBounded(t *testing.T) {
	failIf := func(err error) {
		if err != nil {
			t.Fatalf("%v: %v", testutil.Caller(1), err)
		}
	}

	expectError := func(err error, msg string) {
		if err == nil || !strings.Contains(err.Error(), msg) {
			t.Fatalf("%v: expected an error about %v: got: %v", testutil.Caller(1), msg, err)
		}
	}

	expectLen := func(got, want int) {
		if got != want {
			t.Errorf("%v: got %v, want %v", testutil.Caller(1), got, want)
		}
	}

	expectBuf := func(got []byte, want ...byte) {
		if !bytes.Equal(got, want) {
			t.Errorf("%v: got %v, want %v", testutil.Caller(1), got, want)
		}
	}

	raw := make([]byte, 255)
	for i := 0; i < 255; i++ {
		raw[i] = byte(i)
	}

	rs := bytes.NewReader(raw)

	// Negative offset will fail.
	_, err := deprecated.NewRangeReader(rs, -1, 5)
	expectError(err, "negative position")

	// Seeking past the end of the file is the same as seeking to the end.
	br, err := deprecated.NewRangeReader(rs, 512, 5)
	failIf(err)

	buf := make([]byte, 3)
	n, err := br.Read(buf)
	expectError(err, "EOF")
	expectLen(n, 0)

	br, err = deprecated.NewRangeReader(rs, 48, 5)
	failIf(err)

	n, err = br.Read(buf)
	failIf(err)
	expectLen(n, 3)
	expectBuf(buf, '0', '1', '2')

	buf = make([]byte, 10)
	n, err = br.Read(buf)
	expectError(err, "EOF")
	expectLen(n, 2)
	expectBuf(buf[:n], '3', '4')

	p, err := br.Seek(0, io.SeekStart)
	failIf(err)
	if got, want := p, int64(0); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	n, err = br.Read(buf)
	expectError(err, "EOF")
	expectBuf(buf[:n], '0', '1', '2', '3', '4')

	_, err = br.Seek(2, io.SeekStart)
	failIf(err)
	n, err = br.Read(buf)
	expectError(err, "EOF")
	if got, want := buf[:n], []byte{'2', '3', '4'}; !bytes.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	_, err = br.Seek(-2, io.SeekEnd)
	failIf(err)
	n, err = br.Read(buf)
	expectError(err, "EOF")
	expectBuf(buf[:n], '3', '4')
	_, err = br.Seek(1, io.SeekStart)
	failIf(err)
	_, err = br.Seek(1, io.SeekCurrent)
	failIf(err)
	n, err = br.Read(buf[:2])
	failIf(err)
	expectBuf(buf[:n], '2', '3')

	_, err = br.Seek(100, io.SeekCurrent)
	failIf(err)
	n, err = br.Read(buf[:2])
	expectError(err, "EOF")
	if got, want := n, 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	_, err = br.Seek(-1, io.SeekEnd)
	failIf(err)
	n, err = br.Read(buf[:1])
	expectError(err, "EOF")
	expectBuf(buf[:n], '4')

	// Seeking past the end of the stream is the same as seeking to the end.
	_, err = br.Seek(100, io.SeekEnd)
	failIf(err)
	n, err = br.Read(buf[:1])
	expectError(err, "EOF")
	expectLen(n, 0)
}
