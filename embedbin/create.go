// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package embedbin

import (
	"archive/zip"
	"io"
	"os"
)

// WriteOpt is an option to NewWriter.
type WriteOpt func(*Writer)

// Deflate compresses embedded files.
var Deflate WriteOpt = func(w *Writer) {
	w.embedMethod = zip.Deflate
}

// Writer is used to append embedbin files to an existing binary.
type Writer struct {
	w io.Writer

	embedOffset int64
	embedZ      *zip.Writer
	embedMethod uint16 // no compression by default
}

// NewFileWriter returns a writer that can be used to append embedbin
// files to the binary represented by the provided file.
// NewFileWriter removes any existing embedbin files that may be
// attached to the binary. It relies on content sniffing (see Sniff)
// to determine its offset.
func NewFileWriter(file *os.File) (*Writer, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	embedOffset, err := Sniff(file, info.Size())
	if err != nil {
		return nil, err
	}
	if err = file.Truncate(embedOffset); err != nil {
		return nil, err
	}
	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	return NewWriter(file, embedOffset), nil
}

// NewWriter returns a writer that may be used to append embedbin
// files to the writer w. The writer should be positioned at the end
// of the base binary image.
func NewWriter(w io.Writer, embedOffset int64, opts ...WriteOpt) *Writer {
	ew := Writer{w: w, embedOffset: embedOffset, embedZ: zip.NewWriter(w)}
	for _, opt := range opts {
		opt(&ew)
	}
	return &ew
}

// Create returns a Writer into which the named file should be written.
// The image's contents must be written before the next call to Create or Close.
func (w *Writer) Create(name string) (io.Writer, error) {
	return w.embedZ.CreateHeader(&zip.FileHeader{
		Name:   name,
		Method: w.embedMethod,
	})
}

// Flush flushes the unwritten data to the underlying file.
func (w *Writer) Flush() error {
	return w.embedZ.Flush()
}

// Close should be called after all embedded files have been written.
// No more files can be written after a call to Close.
func (w *Writer) Close() error {
	if err := w.embedZ.Close(); err != nil {
		return err
	}
	_, err := writeFooter(w.w, w.embedOffset)
	return err
}
