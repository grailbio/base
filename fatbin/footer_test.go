// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package fatbin

import (
	"bytes"
	"io"
	"testing"
)

func TestReadWriteFooter(t *testing.T) {
	for _, sz := range []int64{0, 12, 1e12, 1e13 + 4} {
		var b bytes.Buffer
		if _, err := writeFooter(&b, sz); err != nil {
			t.Error(err)
			continue
		}
		off, err := readFooter(bytes.NewReader(b.Bytes()), int64(b.Len()))
		if err != nil {
			t.Error(err)
			continue
		}
		if got, want := off, sz; got != want {
			t.Errorf("got %v, want %v", got, want)
		}

		padded := paddedReaderAt{bytes.NewReader(b.Bytes()), int64(sz) * 100}
		off, err = readFooter(padded, int64(sz)*100+int64(b.Len()))
		if err != nil {
			t.Error(err)
			continue
		}
		if got, want := off, sz; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestCorruptedFooter(t *testing.T) {
	var b bytes.Buffer
	if _, err := writeFooter(&b, 1234); err != nil {
		t.Fatal(err)
	}
	n := b.Len()
	for i := 0; i < n; i++ {
		if i >= n-12 && i < n-8 {
			continue //skip magic
		}
		p := make([]byte, b.Len())
		copy(p, b.Bytes())
		p[i]++
		_, err := readFooter(bytes.NewReader(p), int64(len(p)))
		if got, want := err, ErrCorruptedImage; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

type paddedReaderAt struct {
	io.ReaderAt
	N int64
}

func (r paddedReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	off -= r.N
	for i := range p {
		p[i] = 0
	}
	switch {
	case off < -int64(len(p)):
		return len(p), nil
	case off < 0:
		p = p[-off:]
		off = 0
	}
	return r.ReaderAt.ReadAt(p, off)
}
