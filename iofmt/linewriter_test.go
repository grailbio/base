// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package iofmt_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/grailbio/base/iofmt"
	"github.com/grailbio/testutil/assert"
)

// saveWriter saves the calls made to Write for later comparison.
type saveWriter struct {
	writes [][]byte
}

func (w *saveWriter) Write(p []byte) (int, error) {
	pCopy := make([]byte, len(p))
	copy(pCopy, p)
	w.writes = append(w.writes, pCopy)
	return len(p), nil
}

// TestLineWriter verifies that a LineWriter calls Write on its underlying
// writer with complete lines.
func TestLineWriter(t *testing.T) {
	for _, c := range []struct {
		name      string
		makeLines func() []string
	}{
		{
			name:      "Empty",
			makeLines: func() []string { return []string{} },
		},
		{
			name: "Numbered",
			makeLines: func() []string {
				const Nlines = 1000
				lines := make([]string, Nlines)
				for i := range lines {
					lines[i] = fmt.Sprintf("line %04d", i)
				}
				return lines
			},
		},
		{
			name: "SomeEmpty",
			makeLines: func() []string {
				const Nlines = 1000
				lines := make([]string, Nlines)
				for i := range lines {
					if rand.Intn(2) == 0 {
						continue
					}
					lines[i] = fmt.Sprintf("line %04d", i)
				}
				return lines
			},
		},
		{
			name: "SomeLong",
			makeLines: func() []string {
				const Nlines = 1000
				lines := make([]string, Nlines)
				for i := range lines {
					var b strings.Builder
					fmt.Fprintf(&b, "line %04d:", i)
					for j := 0; j < rand.Intn(100); j++ {
						b.WriteString(" lorem ipsum")
					}
				}
				return lines
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			lines := c.makeLines()
			// bs is a concatenation of all the lines. We write this to a
			// LineWriter in random segments.
			var bs []byte
			for _, line := range lines {
				bs = append(bs, []byte(fmt.Sprintf("%s\n", line))...)
			}
			s := &saveWriter{}
			w := iofmt.LineWriter(s)
			defer func() {
				assert.Nil(t, w.Close())
			}()
			for len(bs) > 0 {
				// Write in random segments.
				n := rand.Intn(20)
				if len(bs) < n {
					n = len(bs)
				}
				m, err := w.Write(bs[:n])
				assert.Nil(t, err)
				assert.EQ(t, m, n)
				bs = bs[n:]
			}
			want := make([][]byte, len(lines))
			for i, line := range lines {
				want[i] = []byte(fmt.Sprintf("%s\n", line))
			}
			assert.EQ(t, s.writes, want)
		})
	}
}

// TestLineWriterClose verifies that (*LineWriter).Close writes any remaining
// partial line to the underlying writer.
func TestLineWriterClose(t *testing.T) {
	for _, c := range []struct {
		name string
		bs   []byte
	}{
		{
			name: "Empty",
			bs:   []byte{},
		},
		{
			name: "PartialOnly",
			bs:   []byte("no terminal newline"),
		},
		{
			name: "Partial",
			bs:   []byte("line 0\nline 1\nline 2\nno terminal newline"),
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			var b bytes.Buffer
			w := iofmt.LineWriter(&b)
			_, err := w.Write(c.bs)
			assert.Nil(t, err)
			assert.Nil(t, w.Close())
			assert.EQ(t, b.Bytes(), c.bs)
		})
	}
}
