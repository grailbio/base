// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package iofmt

import (
	"bytes"
	"io"
)

type lineWriter struct {
	w   io.Writer
	buf []byte
}

// LineWriter returns an io.WriteCloser that only calls w.Write with
// complete lines.  This can be used to make it less likely (without
// locks) for lines to interleave, for example if you are concurrently
// writing lines of text to os.Stdout. This is particularly useful when
// composed with PrefixWriter.
//
//  // Full lines will be written to os.Stdout, so they will be less likely to
//  // be interleaved with other output.
//  linew := LineWriter(os.Stdout)
//  defer func() {
//		_ = linew.Close() // Handle the possible error.
//  }()
//  w := PrefixWriter(linew, "my-prefix: ")
//
// Close will write any remaining partial line to the underlying writer.
func LineWriter(w io.Writer) io.WriteCloser {
	return &lineWriter{w: w}
}

func (w *lineWriter) Write(p []byte) (int, error) {
	var n int
	for {
		i := bytes.Index(p, newline)
		// TODO(jcharumilind): Limit buffer size.
		switch i {
		case -1:
			w.buf = append(w.buf, p...)
			return n + len(p), nil
		default:
			var err error
			if len(w.buf) > 0 {
				w.buf = append(w.buf, p[:i+1]...)
				_, err = w.w.Write(w.buf)
				w.buf = w.buf[:0]
			} else {
				_, err = w.w.Write(p[:i+1])
			}
			n += i + 1
			if err != nil {
				return n, err
			}
			p = p[i+1:]
		}
	}
}

func (w *lineWriter) Close() error {
	if len(w.buf) == 0 {
		return nil
	}
	_, err := w.w.Write(w.buf)
	w.buf = nil
	return err
}
