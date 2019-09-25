// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tsv_test

import (
	"bytes"
	"testing"

	"github.com/grailbio/base/tsv"
)

func TestRowWriter(t *testing.T) {
	var buf bytes.Buffer
	rw := tsv.NewRowWriter(&buf)
	var row struct {
		Bool          bool   `tsv:"true_or_false"`
		String        string `tsv:"name"`
		Int8          int8
		Int16         int16
		Int32         int32
		Int64         int64
		Int           int
		Uint8         uint8
		Uint16        uint16
		Uint32        uint32
		Uint64        uint64
		Uint          uint
		Float32       float32
		Float64       float64
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
	if err := rw.Write(&row); err != nil {
		t.Error(err)
	}
	if err := rw.Flush(); err != nil {
		t.Error(err)
	}
	got := buf.String()
	want := `true_or_false	name	Int8	Int16	Int32	Int64	Int	Uint8	Uint16	Uint32	Uint64	Uint	Float32	Float64
false	abc	0	0	0	0	0	0	0	0	0	0	-3	1e+300
false	def	0	0	0	0	2	0	0	0	0	0	0	1e+300
`
	if got != want {
		t.Errorf("got: %q, want %q", got, want)
	}
}
