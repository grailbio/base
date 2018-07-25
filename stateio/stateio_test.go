// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package stateio

import (
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

func TestStateIO(t *testing.T) {
	file, cleanup := tempfile(t)
	defer cleanup()

	w := NewWriter(file, 0, 0)
	must(t, w.Update(entry(1)))
	must(t, w.Update(entry(2)))

	state, _, updates, err := RestoreFile(file)
	must(t, err)
	if state != nil {
		t.Fatal("non-nil snapshot")
	}
	b, err := updates.Read()
	must(t, err)
	mustEntry(t, 1, b)
	b, err = updates.Read()
	must(t, err)
	mustEntry(t, 2, b)
	mustEOF(t, updates)

	must(t, file.Truncate(0))
	state, _, updates, err = Restore(file, 0)
	if state != nil {
		t.Fatal("non-nil snapshot")
	}
	must(t, err)
	mustEOF(t, updates)

	const N = 100
	w, err = NewFileWriter(file)
	must(t, err)
	for i := 0; i < N; i++ {
		if i%10 == 0 {
			must(t, w.Snapshot(entry(i)))
		} else {
			must(t, w.Update(entry(i)))
		}
		if i%5 == 0 {
			// Reset the writer to make sure that writers
			// resume properly.
			must(t, err)
			w, err = NewFileWriter(file)
			must(t, err)
		}
	}

	state, _, updates, err = RestoreFile(file)
	must(t, err)
	mustEntry(t, N-10, state)
	for i := N - 9; i < N; i++ {
		entry, err := updates.Read()
		must(t, err)
		mustEntry(t, i, entry)
	}
	mustEOF(t, updates)
}

func entry(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewSource(int64(n)))
	for i := range b {
		b[i] = byte(r.Intn(256))
	}
	return b
}

func mustEntry(t *testing.T, n int, b []byte) {
	t.Helper()
	if got, want := len(b), n; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	r := rand.New(rand.NewSource(int64(n)))
	for i := range b {
		if got, want := int(b[i]), r.Intn(256); got != want {
			t.Fatalf("byte %d: got %v, want %v", i, got, want)
		}
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func mustEOF(t *testing.T, r *Reader) {
	t.Helper()
	_, err := r.Read()
	if got, want := err, io.EOF; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func tempfile(t *testing.T) (file *os.File, cleanup func()) {
	t.Helper()
	var err error
	file, err = ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(file.Name())
	cleanup = func() { file.Close() }
	return
}
