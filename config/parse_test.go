// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"strings"
	"testing"
)

// TestParseEmpty verifies that parsing a (logically) empty file is valid.
func TestParseEmpty(t *testing.T) {
	for _, c := range []struct {
		name string
		text string
	}{
		{"Empty", ""},
		{"Whitespace", "\t\n \n\t  \n"},
		{"Semicolons", ";;"},
		{"Mix", " \t \n \n;\n ;"},
	} {
		t.Run(c.name, func(t *testing.T) {
			instances, err := parse(strings.NewReader(c.text))
			if err != nil {
				t.Fatal(err)
			}
			if got, want := len(instances), 0; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	got, err := parse(strings.NewReader(strings.ReplaceAll(`
param x y = "okay"
param y z = 123
param y a = "a"; param y b = b
param y c = nil

param y (
	x = "blah"
	y = 333
	fprec = 0.123456789
	raw = ${"it's json":
12.3}$
)

instance z blah (
	bloop = 123
	negint = -3
	negfloat = -3.14
)

param z x = 89898

instance bigslice/system blah (
	region = "us-west-2"
)

param zero-params ()
`, "$", "`")))
	if err != nil {
		t.Fatal(err)
	}
	want := instances{
		"x": &instance{
			name: "x",
			params: map[string]interface{}{
				"y": "okay",
			},
		},
		"y": &instance{
			name: "y",
			params: map[string]interface{}{
				"x":     "blah",
				"y":     333,
				"z":     123,
				"a":     "a",
				"b":     indirect("b"),
				"c":     indirect(""),
				"fprec": 0.123456789,
				"raw": `{"it's json":
12.3}`,
			},
		},
		"z": &instance{
			name:   "z",
			parent: "blah",
			params: map[string]interface{}{
				"bloop":    123,
				"x":        89898,
				"negint":   -3,
				"negfloat": -3.14,
			},
		},
		"bigslice/system": &instance{
			name:   "bigslice/system",
			parent: "blah",
			params: map[string]interface{}{
				"region": "us-west-2",
			},
		},
		"zero-params": &instance{
			name:   "zero-params",
			parent: "",
			params: map[string]interface{}{},
		},
	}
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	for name, wantInst := range want {
		t.Run(name, func(t *testing.T) {
			syntax := wantInst.SyntaxString(nil)
			insts, err := parse(strings.NewReader(syntax))
			if err != nil {
				t.Fatalf("%v. syntax:\n%s", err, syntax)
			}
			if gotInst := insts[wantInst.name]; !wantInst.Equal(gotInst) {
				t.Errorf("got %v, want %v, syntax:\n%s", got, want, syntax)
			}
		})
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
