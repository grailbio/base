// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package url

import (
	"testing"
)

func TestEncode(t *testing.T) {
	if got, want := Encode("prefix/hi+there"), "prefix/hi%2Bthere"; got != want {
		t.Errorf("wanted %s, got %s", want, got)
	}
	if got, want := Encode("preF:ix/&$@=,? "), "preF%3Aix/%26%24%40%3D%2C%3F%20"; got != want {
		t.Errorf("wanted %s, got %s", want, got)
	}
}

func TestDecode(t *testing.T) {
	got, err := Decode("prefix/hi%2Bthere")
	want := "prefix/hi+there"
	if err != nil {
		t.Errorf("decode error %s", err)
	} else if got != want {
		t.Errorf("wanted %s, got %s", want, got)
	}
	got, err = Decode("preF%3Aix/%26%24%40%3D%2C%3F%20")
	want = "preF:ix/&$@=,? "
	if err != nil {
		t.Errorf("decode error %s", err)
	} else if got != want {
		t.Errorf("wanted %s, got %s", want, got)
	}
}
