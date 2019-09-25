// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package tsv

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Writer provides an efficient and concise way to append a field at a time to
// a TSV.  However, note that it does NOT have a Write() method; the interface
// is deliberately restricted.
//
// We force this to fill at least one cacheline to prevent false sharing when
// make([]Writer, parallelism) is used.
type Writer struct {
	w       *bufio.Writer
	line    []byte
	padding [32]byte // nolint: megacheck, structcheck, staticcheck
}

// NewWriter creates a new tsv.Writer from an io.Writer.
func NewWriter(w io.Writer) (tw *Writer) {
	return &Writer{
		w:    bufio.NewWriter(w),
		line: make([]byte, 0, 256),
	}
}

// WriteString appends the given string and a tab to the current line.  (It is
// safe to use this to write multiple fields at a time.)
func (w *Writer) WriteString(s string) {
	w.line = append(w.line, s...)
	w.line = append(w.line, '\t')
}

// WriteBytes appends the given []byte and a tab to the current line.
func (w *Writer) WriteBytes(s []byte) {
	w.line = append(w.line, s...)
	w.line = append(w.line, '\t')
}

// WriteUint32 converts the given uint32 to a string, and appends that and a
// tab to the current line.
func (w *Writer) WriteUint32(ui uint32) {
	w.WriteUint64(uint64(ui))
}

// WriteInt64 converts the given int64 to a string, and appends that and a
// tab to the current line.
func (w *Writer) WriteInt64(i int64) {
	w.line = strconv.AppendInt(w.line, i, 10)
	w.line = append(w.line, '\t')
}

// WriteUint64 converts the given uint64 to a string, and appends that and a
// tab to the current line.
func (w *Writer) WriteUint64(ui uint64) {
	w.line = strconv.AppendUint(w.line, ui, 10)
	w.line = append(w.line, '\t')
}

// WriteFloat64 converts the given float64 to a string with the given
// strconv.AppendFloat parameters, and appends that and a tab to the current
// line.
func (w *Writer) WriteFloat64(f float64, fmt byte, prec int) {
	w.line = strconv.AppendFloat(w.line, f, fmt, prec, 64)
	w.line = append(w.line, '\t')
}

// WriteByte appends the given literal byte (no number->string conversion) and
// a tab to the current line.
func (w *Writer) WriteByte(b byte) {
	w.line = append(w.line, b)
	w.line = append(w.line, '\t')
}

// WritePartialString appends a string WITHOUT the usual subsequent tab.  It
// must be followed by a non-Partial Write at some point to end the field;
// otherwise EndLine will clobber the last character.
func (w *Writer) WritePartialString(s string) {
	w.line = append(w.line, s...)
}

// WritePartialBytes appends a []byte WITHOUT the usual subsequent tab.  It
// must be followed by a non-Partial Write at some point to end the field;
// otherwise EndLine will clobber the last character.
func (w *Writer) WritePartialBytes(s []byte) {
	w.line = append(w.line, s...)
}

// WritePartialUint32 converts the given uint32 to a string, and appends that
// WITHOUT the usual subsequent tab.  It must be followed by a non-Partial
// Write at some point to end the field; otherwise EndLine will clobber the
// last character.
func (w *Writer) WritePartialUint32(ui uint32) {
	w.line = strconv.AppendUint(w.line, uint64(ui), 10)
}

// WriteCsvUint32 converts the given uint32 to a string, and appends that and a
// comma to the current line.
func (w *Writer) WriteCsvUint32(ui uint32) {
	w.line = strconv.AppendUint(w.line, uint64(ui), 10)
	w.line = append(w.line, ',')
}

// WriteCsvByte appends the given literal byte (no number->string conversion)
// and a comma to the current line.
func (w *Writer) WriteCsvByte(b byte) {
	w.line = append(w.line, b)
	w.line = append(w.line, ',')
}

// (Other Csv functions will be added as they're needed.)

// EndCsv finishes the current comma-separated field, converting the last comma
// to a tab.  It must be nonempty.
func (w *Writer) EndCsv() {
	w.line[len(w.line)-1] = '\t'
}

// EndLine finishes the current line.  It must be nonempty.
func (w *Writer) EndLine() (err error) {
	w.line[len(w.line)-1] = '\n'
	// Tried making less frequent Write calls, doesn't seem to help.
	_, err = w.w.Write(w.line)
	w.line = w.line[:0]
	return
}

// Flush flushes all finished lines.
func (w *Writer) Flush() error {
	return w.w.Flush()
}

// Copy appends the entire contents of the given io.Reader (assumed to be
// another TSV file).
func (w *Writer) Copy(r io.Reader) error {
	if len(w.line) != 0 {
		return fmt.Errorf("Writer.Copy: current line is nonempty")
	}
	_, err := io.Copy(w.w, r)
	return err
}
