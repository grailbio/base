// Package compress provides convenience functions for creating compressors and
// uncompressors based on filenames.
package compress

import (
	"bytes"
	"compress/bzip2"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/grailbio/base/compress/zstd"
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

// nopWriteCloser adds a noop Closer to io.Writer.
type nopWriteCloser struct{ io.Writer }

func (w *nopWriteCloser) Close() error { return nil }

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
	return false
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

// https://tools.ietf.org/html/rfc8478
func isZstdHeader(buf []byte) bool {
	if len(buf) < 4 {
		return false
	}
	if buf[0] != 0x28 || buf[1] != 0xB5 || buf[2] != 0x2F || buf[3] != 0xFD {
		return false
	}
	return true
}

// NewReader creates an uncompressing reader by reading the first few bytes of
// the input and finding a magic header for either gzip, zstd, bzip2. If the
// magic header is found , it returns an uncompressing ReadCloser and
// true. Else, it returns ioutil.NopCloser(r) and false.
//
// CAUTION: this function will misbehave when the input is a binary string that
// happens to have the same magic gzip, zstd, or bzip2 header.  Thus, you should
// use this function only when the input is expected to be ASCII.
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
	if isZstdHeader(buf.Bytes()) {
		zr, err := zstd.NewReader(m)
		if err != nil {
			return &errorReader{err}, false
		}
		return zr, true
	}
	if isBzip2Header(buf.Bytes()) {
		return ioutil.NopCloser(bzip2.NewReader(m)), true
	}
	return ioutil.NopCloser(m), false
}

// NewReaderPath creates a reader that uncompresses data read from the given
// reader.  The compression format is determined by the pathname extensions. If
// the pathname ends with one of the following extensions, it creates an
// uncompressing ReadCloser and returns true.
//
//  .gz => gzip format
//  .zst => zstd format
//  .bz2 => bz2 format
//
// For other extensions, this function returns an ioutil.NopCloser(r) and false.
//
// The caller must close the ReadCloser after use. For some file formats,
// Close() is the only place that reports file corruption.
func NewReaderPath(r io.Reader, path string) (io.ReadCloser, bool) {
	switch fileio.DetermineType(path) {
	case fileio.Gzip:
		gz, err := zlibng.NewReader(r)
		if err != nil {
			return file.NewError(err), false
		}
		return gz, true
	case fileio.Zstd:
		zr, err := zstd.NewReader(r)
		if err != nil {
			return file.NewError(err), false
		}
		return zr, true
	case fileio.Bzip2:
		return ioutil.NopCloser(bzip2.NewReader(r)), true
	}
	return ioutil.NopCloser(r), false
}

// NewWriterPath creates a WriteCloser that compresses data.  The compression
// format is determined by the pathname extensions. If the pathname ends with
// one of the following extensions, it creates an compressing WriteCloser and
// returns true.
//
//  .gz => gzip format
//  .zst => zstd format
//
// For other extensions, this function creates a noop WriteCloser and returns
// false.  The caller must close the WriteCloser after use.
func NewWriterPath(w io.Writer, path string) (io.WriteCloser, bool) {
	switch fileio.DetermineType(path) {
	case fileio.Gzip:
		return gzip.NewWriter(w), true
	case fileio.Zstd:
		zw, err := zstd.NewWriter(w)
		if err != nil {
			return file.NewError(err), false
		}
		return zw, true
	case fileio.Bzip2:
		return file.NewError(fmt.Errorf("%s: bzip2 writer not supported", path)), false
	}
	return &nopWriteCloser{w}, false
}
