// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tsv_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/grailbio/base/tsv"
)

func TestRowWriter(t *testing.T) {
	var buf bytes.Buffer
	rw := tsv.NewRowWriter(&buf)
	type embedded struct {
		EmbeddedString string  `tsv:"estring"`
		EmbeddedFloat  float64 `tsv:"efloat,fmt=0.3f"`
	}
	var row struct {
		Bool    bool   `tsv:"true_or_false"`
		String  string `tsv:"name"`
		Int8    int8
		Int16   int16
		Int32   int32
		Int64   int64
		Int     int
		Uint8   uint8
		Uint16  uint16
		Uint32  uint32
		Uint64  uint64
		Uint    uint
		Float32 float32
		Float64 float64
		embedded
		skippedString string
		skippedFunc   func()
	}
	row.String = "abc"
	row.Float32 = -3
	row.Float64 = 1e300
	if err := rw.Write(&row); err != nil {
		t.Error(err)
	}
	row.String = "def"
	row.Int = 2
	row.Float32 = 0
	row.EmbeddedString = "estring"
	row.EmbeddedFloat = 0.123456
	if err := rw.Write(&row); err != nil {
		t.Error(err)
	}
	if err := rw.Flush(); err != nil {
		t.Error(err)
	}
	got := buf.String()
	want := `true_or_false	name	Int8	Int16	Int32	Int64	Int	Uint8	Uint16	Uint32	Uint64	Uint	Float32	Float64	estring	efloat
false	abc	0	0	0	0	0	0	0	0	0	0	-3	1e+300		0.000
false	def	0	0	0	0	2	0	0	0	0	0	0	1e+300	estring	0.123
`
	if got != want {
		t.Errorf("got: %q, want %q", got, want)
	}
}

func ExampleRowWriter() {
	type rowTyp struct {
		Foo float64 `tsv:"foo,fmt=.2f"`
		Bar float64 `tsv:"bar,fmt=.3f"`
		Baz float64
	}
	rows := []rowTyp{
		{Foo: 0.1234, Bar: 0.4567, Baz: 0.9876},
		{Foo: 1.1234, Bar: 1.4567, Baz: 1.9876},
	}
	var buf bytes.Buffer
	w := tsv.NewRowWriter(&buf)
	for i := range rows {
		if err := w.Write(&rows[i]); err != nil {
			panic(err)
		}
	}
	if err := w.Flush(); err != nil {
		panic(err)
	}
	fmt.Print(string(buf.Bytes()))

	// Output:
	// foo	bar	Baz
	// 0.12	0.457	0.9876
	// 1.12	1.457	1.9876
}
