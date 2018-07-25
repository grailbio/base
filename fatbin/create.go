// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package fatbin

import (
	"archive/zip"
	"io"
	"os"
)

// Writer is used to append fatbin images to an existing binary.
type Writer struct {
	z            *zip.Writer
	w            io.Writer
	off          int64
	goos, goarch string
}

// NewFileWriter returns a writer that can be used to append fatbin
// images to the binary represented by the provided file.
// NewFileWriter removes any existing fatbin images that may be
// attached to the binary. It relies on content sniffing (see Sniff)
// to determine its offset.
func NewFileWriter(file *os.File) (*Writer, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	goos, goarch, offset, err := Sniff(file, info.Size())
	if err != nil {
		return nil, err
	}
	if err := file.Truncate(offset); err != nil {
		return nil, err
	}
	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	return NewWriter(file, offset, goos, goarch), nil
}

// NewWriter returns a writer that may be used to append fatbin
// images to the writer w. The writer should be positioned at the end
// of the base binary image.
func NewWriter(w io.Writer, offset int64, goos, goarch string) *Writer {
	return &Writer{z: zip.NewWriter(w), w: w, off: offset, goos: goos, goarch: goarch}
}

// Create returns a Writer into which the image for the provided goos
// and goarch should be written. The image's contents must be written
// before the next call to Create or Close.
func (w *Writer) Create(goos, goarch string) (io.Writer, error) {
	return w.z.Create(goos + "/" + goarch)
}

// Flush flushes the unwritten data to the underlying file.
func (w *Writer) Flush() error {
	return w.z.Flush()
}

// Close should be called after all images have been written. No more
// images can be written after a call to Close.
func (w *Writer) Close() error {
	if err := w.z.SetComment(w.goos + "/" + w.goarch); err != nil {
		return err
	}
	if err := w.z.Close(); err != nil {
		return err
	}
	_, err := writeFooter(w.w, w.off)
	return err
}
