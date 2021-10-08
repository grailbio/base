package ioctx

import (
	"context"
	"io"
)

type (
	stdReader   struct{ io.Reader }
	stdCloser   struct{ io.Closer }
	stdReaderAt struct{ io.ReaderAt }
)

// FromStdReader wraps io.Reader as Reader.
func FromStdReader(r io.Reader) Reader { return stdReader{r} }

func (r stdReader) Read(_ context.Context, dst []byte) (n int, err error) {
	return r.Reader.Read(dst)
}

// FromStdCloser wraps io.Closer as Closer.
func FromStdCloser(c io.Closer) Closer { return stdCloser{c} }

func (c stdCloser) Close(context.Context) error { return c.Closer.Close() }

// FromStdReadCloser wraps io.ReadCloser as ReadCloser.
func FromStdReadCloser(rc io.ReadCloser) ReadCloser {
	return struct {
		Reader
		Closer
	}{FromStdReader(rc), FromStdCloser(rc)}
}

// FromStdReaderAt wraps io.ReaderAt as ReaderAt.
func FromStdReaderAt(r io.ReaderAt) ReaderAt { return stdReaderAt{r} }

func (r stdReaderAt) ReadAt(_ context.Context, dst []byte, off int64) (n int, err error) {
	return r.ReaderAt.ReadAt(dst, off)
}
