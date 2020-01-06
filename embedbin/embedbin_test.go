// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package embedbin

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestEmbedbin(t *testing.T) {
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
	r, err := self.OpenBase()
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
}

func TestEmbedbinNonExist(t *testing.T) {
	self, err := Self()
	if err != nil {
		t.Fatal(err)
	}
	_, err = self.Open("nonexistent")
	if got, want := err, ErrNoSuchFile; got != want {
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

	size, err := Sniff(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := size, info.Size(); got != want {
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
	dw, err := w.Create("darwin/amd64")
	must(t, err)
	_, err = dw.Write([]byte("darwin/amd64"))
	must(t, err)
	dw, err = w.Create("darwin/386")
	must(t, err)
	_, err = dw.Write([]byte("darwin/386"))
	must(t, err)
	must(t, w.Close())
	info, err := f.Stat()
	must(t, err)
	r, err := OpenFile(f, info.Size())
	must(t, err)

	cases := []struct {
		base bool
		name string
		body []byte
	}{
		{base: true, body: svelteLinuxElfBinary},
		{name: "darwin/amd64", body: []byte("darwin/amd64")},
		{name: "darwin/386", body: []byte("darwin/386")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var rc io.ReadCloser
			if c.base {
				rc, err = r.OpenBase()
			} else {
				rc, err = r.Open(c.name)
			}
			if err != nil {
				t.Fatal(err)
			}
			mustBytes(t, rc, c.body)
			must(t, rc.Close())
			if c.base {
				return
			}
			info, ok := r.Stat(c.name)
			if !ok {
				t.Errorf("%s/%t: not found", c.name, c.base)
				return
			}
			if got, want := info.Size, int64(len(c.body)); got != want {
				t.Errorf("%s: got %v, want %v", c.name, got, want)
			}
		})
	}

	_, err = r.Open("nonexistent")
	if got, want := err, ErrNoSuchFile; got != want {
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
