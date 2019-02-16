// Package compress provides convenience functions for creating compressors and
// uncompressors based on filenames.
package compress

import (
	"bytes"
	"compress/bzip2"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/fileio"
	"github.com/klauspost/compress/gzip"
	"github.com/yasushi-saito/zlibng"
)

// errorReader is a ReadCloser implementation that always returns the given
// error.
type errorReader struct{ err error }

func (r *errorReader) Read(buf []byte) (int, error) { return 0, r.err }
func (r *errorReader) Close() error                 { return r.err }

func isBzip2Header(buf []byte) bool {
	// https://www.forensicswiki.org/wiki/Bzip2
	if len(buf) < 10 {
		return false
	}
	if !(buf[0] == 'B' && buf[1] == 'Z' && buf[2] == 'h' && buf[3] >= '1' && buf[3] <= '9') {
		return false
	}
	if buf[4] == 0x31 && buf[5] == 0x41 &&
		buf[6] == 0x59 && buf[7] == 0x26 &&
		buf[8] == 0x53 && buf[9] == 0x59 { // block magic
		return true
	}
	if buf[4] == 0x17 && buf[5] == 0x72 &&
		buf[6] == 0x45 && buf[7] == 0x38 &&
		buf[8] == 0x50 && buf[9] == 0x90 { // eos magic, happens only for an empty bz2 file.
		return true
	}
	return true
}

func isGzipHeader(buf []byte) bool {
	if len(buf) < 10 {
		return false
	}
	if !(buf[0] == 0x1f && buf[1] == 0x8b) {
		return false
	}
	if !(buf[2] <= 3 || buf[2] == 8) {
		return false
	}
	if (buf[3] & 0xc0) != 0 {
		return false
	}
	if !(buf[9] <= 0xd || buf[9] == 0xff) {
		return false
	}
	return true
}

// NewReader creates an uncompressing reader by reading the first few bytes of
// the input and finding a magic header for either gzip or bzip2. If the magic
// header is found , it returns an uncompressing ReadCloser and true. Else, it
// returns ioutil.NopCloser(r) and false.
//
// CAUTION: this function will misbehave when the input is a binary string that
// happens to have the same magic gzip or bzip2 header.  Thus, you should use
// this function only when the input is expected to be ASCII.
func NewReader(r io.Reader) (io.ReadCloser, bool) {
	buf := bytes.Buffer{}
	_, err := io.CopyN(&buf, r, 128)
	var m io.Reader
	switch err {
	case io.EOF:
		m = &buf
	case nil:
		m = io.MultiReader(&buf, r)
	default:
		m = io.MultiReader(&buf, &errorReader{err})
	}
	if isGzipHeader(buf.Bytes()) {
		z, err := zlibng.NewReader(m)
		if err != nil {
			return &errorReader{err}, false
		}
		return z, true
	}
	if isBzip2Header(buf.Bytes()) {
		return ioutil.NopCloser(bzip2.NewReader(m)), true
	}
	return ioutil.NopCloser(m), false
}

// NewReaderPath creates a reader that uncompresses data read from the given
// reader.  The compression format is determined by the pathname extensions. The
// following extensions are recognized:
//
//  .gz => gzip format
//  .bz2 => bz2 format
//
// For other extensions, this function returns nil.
//
// If the caller receives a non-nil reader from this function, it must close the
// reader after use. For some file formats, Close() is the only place that
// reports file corruption.
func NewReaderPath(r io.Reader, path string) io.ReadCloser {
	switch fileio.DetermineType(path) {
	case fileio.Gzip:
		gz, err := zlibng.NewReader(r)
		if err != nil {
			return file.NewError(err)
		}
		return gz
	case fileio.Bzip2:
		return ioutil.NopCloser(bzip2.NewReader(r))
	}
	return nil
}

// NewWriterPath creates a WriteCloser that compresses data.  The compression
// format is determined by the pathname extensions. The following extension is
// recognized:
//
//  .gz => gzip format
//
// For other extensions, this function returns nil. If this function returns a
// non-nil writecloser, the caller must call Close() once after writing all the
// data.
func NewWriterPath(w io.Writer, path string) io.WriteCloser {
	switch fileio.DetermineType(path) {
	case fileio.Gzip:
		return gzip.NewWriter(w)
	case fileio.Bzip2:
		return file.NewError(fmt.Errorf("%s: bzip2 writer not supported", path))
	}
	return nil
}
