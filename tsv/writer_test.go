// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tsv_test

import (
	"bytes"
	"testing"

	"github.com/grailbio/base/tsv"
)

func TestWriter(t *testing.T) {
	var buf bytes.Buffer
	tw := tsv.NewWriter(&buf)
	tw.WriteString("field1")
	tw.WriteUint32(2)
	tw.WritePartialString("field")
	tw.WriteByte('3')
	tw.WriteBytes([]byte{'f', 'i', 'e', 'l', 'd', '4'})
	tw.WriteFloat64(1.2345, 'G', 6)
	tw.WriteInt64(123456)
	tw.EndLine()
	tw.Flush()
	got := buf.String()
	want := "field1\t2\tfield3\tfield4\t1.2345\t123456\n"
	if got != want {
		t.Errorf("got: %q, want %q", got, want)
	}
}
