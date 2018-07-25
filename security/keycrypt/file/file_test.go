// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package file

import (
	"io/ioutil"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/grailbio/testutil"
)

func TestFile(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "file")
	defer cleanup()
	c := &crypt{dir}
	s := c.Lookup("foo")
	b, err := s.Get()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(b), 0; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	_, files := testutil.ListRecursively(t, dir)
	if got, want := len(files), 0; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	hello := []byte("hello world")
	if err := s.Put(hello); err != nil {
		t.Fatal(err)
	}
	b, err = s.Get()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := b, hello; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	_, files = testutil.ListRecursively(t, dir)
	if got, want := files, []string{filepath.Join(dir, "foo")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	b, err = ioutil.ReadFile(filepath.Join(dir, "foo"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := b, hello; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
