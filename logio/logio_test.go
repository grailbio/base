// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package logio

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"
)

func TestLogIO(t *testing.T) {
	sizes := records(100, 128<<10)
	// Make sure that there are some "tricky" sizes in here, to exercise
	// all of the code paths.
	sizes[10] = Blocksz                // doesn't fit by a small margin
	sizes[11] = Blocksz - headersz     // exact fit
	sizes[12] = Blocksz - headersz - 2 // underfit by less than headersz; next entry requires padding

	var (
		buf     bytes.Buffer
		scratch []byte
	)
	w := NewWriter(&buf, 0)
	for _, sz := range sizes {
		scratch = data(scratch, sz)
		must(t, w.Append(scratch))
	}

	r := NewReader(&buf, 0)
	for i, sz := range sizes {
		t.Logf("record %d size %d", i, sz)
		rec, err := r.Read()
		mustf(t, err, "record %d (size %d)", i, sz)
		if got, want := len(rec), sz; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		mustData(t, rec)
	}
	mustEOF(t, r)
}

func TestResync(t *testing.T) {
	var (
		sizes   = records(20, 128<<10)
		scratch []byte
		buf     bytes.Buffer
	)
	w := NewWriter(&buf, 0)
	for _, sz := range sizes {
		scratch = data(scratch, sz)
		must(t, w.Append(scratch))
	}
	buf.Bytes()[1]++
	r := NewReader(&buf, 0)
	var i int
	for i = range sizes {
		rec, err := r.Read()
		if err == ErrCorrupted {
			break
		}
		must(t, err)
		if got, want := len(rec), sizes[i]; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		mustData(t, rec)
	}
	if i == len(sizes) {
		t.Fatal("corrupted record not detected")
	}
	rec, err := r.Read()
	mustf(t, err, "failed to recover from corrupted record")
	mustData(t, rec)
	j := i
	for ; i < len(sizes); i++ {
		if len(rec) == sizes[i] {
			i++
			break
		}
	}
	if i == len(sizes) {
		t.Fatal("failed to resync")
	}
	t.Logf("skipped %d records", i-j)
	for ; i < len(sizes); i++ {
		rec, err := r.Read()
		mustf(t, err, "record %d/%d", i, len(sizes))
		mustData(t, rec)
	}
	mustEOF(t, r)
}

func TestRewind(t *testing.T) {
	sizes := records(50, 128<<10)
	var (
		buf     bytes.Buffer
		scratch []byte
	)
	w := NewWriter(&buf, 0)
	for _, sz := range sizes {
		scratch = data(scratch, sz)
		must(t, w.Append(scratch))
	}
	var (
		rd  = bytes.NewReader(buf.Bytes())
		off = int64(rd.Len())
	)
	for n := 1; n <= 10; n++ {
		var err error
		off, err = Rewind(rd, off)
		must(t, err)
		// Check that Rewind also seeked rd to the correct offset.
		seekPos, err := rd.Seek(0, io.SeekCurrent)
		must(t, err)
		if got, want := off, seekPos; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		r := NewReader(rd, off)
		for i, sz := range sizes[len(sizes)-n:] {
			rec, err := r.Read()
			must(t, err)
			if got, want := len(rec), sz; got != want {
				t.Fatalf("%d,%d: got %v, want %v", n, i, got, want)
			}
			mustData(t, rec)
		}
		mustEOF(t, r)
	}
}

func records(n, max int) []int {
	if n > max {
		panic("n > max")
	}
	var (
		recs   = make([]int, n)
		stride = max / n
	)
	for i := range recs {
		recs[i] = 1 + stride*i
	}
	r := rand.New(rand.NewSource(int64(n + max)))
	r.Shuffle(n, func(i, j int) { recs[i], recs[j] = recs[j], recs[i] })
	return recs
}

func data(scratch []byte, n int) []byte {
	if n <= cap(scratch) {
		scratch = scratch[:n]
	} else {
		scratch = make([]byte, n)
	}
	r := rand.New(rand.NewSource(int64(n)))
	for i := range scratch {
		scratch[i] = byte(r.Intn(256))
	}
	return scratch
}

func mustData(t *testing.T, b []byte) {
	t.Helper()
	r := rand.New(rand.NewSource(int64(len(b))))
	for i := range b {
		if got, want := int(b[i]), r.Intn(256); got != want {
			t.Fatalf("byte %d: got %v, want %v", i, got, want)
		}
	}
}

func mustEOF(t *testing.T, r *Reader) {
	t.Helper()
	_, err := r.Read()
	if got, want := err, io.EOF; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func mustf(t *testing.T, err error, format string, v ...interface{}) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", fmt.Sprintf(format, v...), err)
	}
}
