// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package fatbin

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
)

func TestFatbin(t *testing.T) {
	filename, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	body, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	self, err := Self()
	if err != nil {
		t.Fatal(err)
	}
	r, err := self.Open(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	embedded, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, embedded) {
		t.Error("content mismatch")
	}

	info, ok := self.Stat(runtime.GOOS, runtime.GOARCH)
	if !ok {
		t.Fatal(runtime.GOOS, "/", runtime.GOARCH, ": not found")
	}
	if got, want := info.Size, int64(len(embedded)); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestFatbinNonExist(t *testing.T) {
	self, err := Self()
	if err != nil {
		t.Fatal(err)
	}
	_, err = self.Open("nonexistent", "nope")
	if got, want := err, ErrNoSuchImage; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSniff(t *testing.T) {
	filename, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	goos, goarch, size, err := Sniff(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := goarch, runtime.GOARCH; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := goos, runtime.GOOS; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := size, info.Size(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestLinuxElf(t *testing.T) {
	r := bytes.NewReader(svelteLinuxElfBinary)
	goos, goarch, size, err := Sniff(r, int64(r.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := goos, "linux"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := goarch, "386"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := size, len(svelteLinuxElfBinary); got != int64(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCreate(t *testing.T) {
	f, err := ioutil.TempFile("", "")
	must(t, err)
	_, err = f.Write(svelteLinuxElfBinary)
	must(t, err)
	w, err := NewFileWriter(f)
	must(t, err)
	dw, err := w.Create("darwin", "amd64")
	must(t, err)
	_, err = dw.Write([]byte("darwin/amd64"))
	must(t, err)
	dw, err = w.Create("darwin", "386")
	must(t, err)
	_, err = dw.Write([]byte("darwin/386"))
	must(t, err)
	must(t, w.Close())
	info, err := f.Stat()
	must(t, err)
	r, err := OpenFile(f, info.Size())
	must(t, err)

	cases := []struct {
		goos, goarch string
		body         []byte
	}{
		{"linux", "386", svelteLinuxElfBinary},
		{"darwin", "amd64", []byte("darwin/amd64")},
		{"darwin", "386", []byte("darwin/386")},
	}
	for _, c := range cases {
		rc, err := r.Open(c.goos, c.goarch)
		if err != nil {
			t.Fatal(err)
		}
		mustBytes(t, rc, c.body)
		must(t, rc.Close())
		info, ok := r.Stat(c.goos, c.goarch)
		if !ok {
			t.Error(c.goos, "/", c.goarch, ": not found")
			continue
		}
		if got, want := info.Size, int64(len(c.body)); got != want {
			t.Errorf("%s/%s: got %v, want %v", c.goos, c.goarch, got, want)
		}
	}

	_, err = r.Open("test", "nope")
	if got, want := err, ErrNoSuchImage; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func mustBytes(t *testing.T, r io.Reader, want []byte) {
	t.Helper()
	got, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("got %s, want %s", got, want)
	}
}
