// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package embedbin

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

var (
	selfOnce sync.Once
	self     *Reader
	selfErr  error
)

var (
	// ErrNoSuchFile is returned when the embedbin does not contain an
	// embedded file with the requested name.
	ErrNoSuchFile = errors.New("embedded file does not exist")
	// ErrCorruptedImage is returned when the embedbin image has been
	// corrupted.
	ErrCorruptedImage = errors.New("corrupted embedbin image")
)

// Info provides information for an embedded file.
type Info struct {
	Name string
	Size int64
}

func (info Info) String() string {
	return fmt.Sprintf("%s: %d", info.Name, info.Size)
}

// Reader reads images from an embedbin.
type Reader struct {
	base io.ReaderAt

	embedOffset int64
	embedZ      *zip.Reader
}

// Self reads the currently executing binary image as an embedbin and
// returns a reader to it.
func Self() (*Reader, error) {
	selfOnce.Do(func() {
		filename, err := os.Executable()
		if err != nil {
			selfErr = err
			return
		}
		f, err := os.Open(filename)
		if err != nil {
			selfErr = err
			return
		}
		info, err := f.Stat()
		if err != nil {
			selfErr = err
			return
		}
		embedOffset, err := Sniff(f, info.Size())
		if err != nil {
			selfErr = err
			return
		}
		self, selfErr = NewReader(f, embedOffset, info.Size())
	})
	return self, selfErr
}

// OpenFile parses the provided ReaderAt with the provided size. The
// file's contents are parsed to determine the offset of the embedbin's
// archive. OpenFile returns an error if the file is not an embedbin.
func OpenFile(r io.ReaderAt, size int64) (*Reader, error) {
	offset, err := Sniff(r, size)
	if err != nil {
		return nil, err
	}
	return NewReader(r, offset, size)
}

// NewReader returns a new embedbin reader from the provided reader.
func NewReader(r io.ReaderAt, embedOffset, totalSize int64) (*Reader, error) {
	rd := &Reader{
		base:        io.NewSectionReader(r, 0, embedOffset),
		embedOffset: embedOffset,
	}
	if embedOffset == totalSize {
		return rd, nil
	}
	var err error
	rd.embedZ, err = zip.NewReader(io.NewSectionReader(r, embedOffset, totalSize-embedOffset), totalSize-embedOffset)
	if err != nil {
		return nil, err
	}
	return rd, nil
}

// List returns information about embedded files.
func (r *Reader) List() []Info {
	if r.embedZ == nil {
		return nil
	}
	infos := make([]Info, len(r.embedZ.File))
	for i, f := range r.embedZ.File {
		infos[i] = Info{
			Name: f.Name,
			Size: int64(f.UncompressedSize64),
		}
	}
	return infos
}

// Open returns a ReadCloser for the original executable, without appended
// embedded files.
func (r *Reader) OpenBase() (io.ReadCloser, error) {
	return ioutil.NopCloser(io.NewSectionReader(r.base, 0, 1<<63-1)), nil
}

// Open returns a ReadCloser for the named embedded file.
// Open returns ErrNoSuchImage if the embedbin does not contain the file.
func (r *Reader) Open(name string) (io.ReadCloser, error) {
	if r.embedZ == nil {
		return nil, ErrNoSuchFile
	}
	for _, f := range r.embedZ.File {
		if f.Name == name {
			return f.Open()
		}
	}
	return nil, ErrNoSuchFile
}

// StatBase returns the information for the base image.
func (r *Reader) StatBase() Info {
	return Info{Size: r.embedOffset}
}

// Stat returns the information for the named embedded file.
// It returns a boolean indicating whether the requested file was found.
func (r *Reader) Stat(name string) (info Info, ok bool) {
	info.Name = name
	for _, f := range r.embedZ.File {
		if f.Name == name {
			info.Size = int64(f.UncompressedSize64)
			ok = true
			return
		}
	}
	return
}

// Sniff sniffs a binary's embedbin offset. Sniff returns errors
// returned by the provided reader, or ErrCorruptedImage if the binary is identified
// as an embedbin image with a checksum mismatch.
func Sniff(r io.ReaderAt, size int64) (offset int64, err error) {
	offset, err = readFooter(r, size)
	if err == errNoFooter {
		err = nil
		offset = size
	}
	return
}
