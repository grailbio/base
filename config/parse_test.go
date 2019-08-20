// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	insts, err := parse(strings.NewReader(`
param x y = "okay"
param y z = 123
param y a = "a"; param y b = b

param y (
	x = "blah"
	y = 333
)

instance z blah (
	bloop = 123
)

param z x = 89898

instance bigslice/system blah (
	region = "us-west-2"
)

`))
	if err != nil {
		t.Fatal(err)
	}
	for _, inst := range insts {
		t.Log(inst.SyntaxString(nil))
	}
	if got, want := insts, (instances{
		"x": &instance{
			name: "x",
			params: map[string]interface{}{
				"y": "okay",
			},
		},
		"y": &instance{
			name: "y",
			params: map[string]interface{}{
				"x": "blah",
				"y": 333,
				"z": 123,
				"a": "a",
				"b": indirect("b"),
			},
		},
		"z": &instance{
			name:   "z",
			parent: "blah",
			params: map[string]interface{}{
				"bloop": 123,
				"x":     89898,
			},
		},
		"bigslice/system": &instance{
			name:   "bigslice/system",
			parent: "blah",
			params: map[string]interface{}{
				"region": "us-west-2",
			},
		},
	}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

}

func TestParseError(t *testing.T) {
	testError(t, `parm x y = 1`, `parse error: <input>:1:1: unrecognized toplevel clause: parm`)
	testError(t, `param x _y = "hey"`, `parse error: <input>:1:9: unexpected: "_"`)
	testError(t, `param x 123 = blah`, `parse error: <input>:1:9: unexpected: Int`)
	testError(t, `param x y z`, `parse error: <input>:1:11: expected "="`)
}

func testError(t *testing.T, s, expect string) {
	t.Helper()
	_, err := parse(strings.NewReader(s))
	if err == nil {
		t.Error("expected error")
		return
	}
	if got, want := err.Error(), expect; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
