// Package zstd wraps github.com/DataDog/zstd and
// github.com/klauspost/compress/zstd.  It uses DataDog/zstd in cgo mode, and
// klauspost/compress/zstd in noncgo mode.

// +build cgo

package zstd

import (
	"io"

	cgozstd "github.com/DataDog/zstd"
)

// Compress compresses the given source data.  Scratch can be passed to prevent
// prevent allocation.  If it is too small, or if nil is passed, a new buffer
// will be allocated and returned.
func Compress(scratch []byte, in []byte) ([]byte, error) {
	return cgozstd.Compress(scratch, in)
}

// Decompress uncompresses the given source data.  Scratch can be passed to
// prevent allocation.  If it is too small, or if nil is passed, a new buffer
// will be allocated and returned.
func Decompress(scratch []byte, in []byte) ([]byte, error) {
	return cgozstd.Decompress(scratch, in)
}

// NewReader creates a ReadCloser that uncompresses data.  The returned object
// must be Closed after use.
func NewReader(r io.Reader) (io.ReadCloser, error) {
	return cgozstd.NewReader(r), nil
}

// NewWriter creates a WriterCloser that compresses data.  The returned object
// must be Closed after use.
func NewWriter(w io.Writer) (io.WriteCloser, error) {
	return cgozstd.NewWriter(w), nil
}
