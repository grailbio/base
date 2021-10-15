package ioctx

import (
	"context"
	"io"
)

type (
	fromStdReader   struct{ io.Reader }
	fromStdCloser   struct{ io.Closer }
	fromStdReaderAt struct{ io.ReaderAt }

	toStdReader struct {
		ctx context.Context
		Reader
	}
)

// FromStdReader wraps io.Reader as Reader.
func FromStdReader(r io.Reader) Reader { return fromStdReader{r} }

func (r fromStdReader) Read(_ context.Context, dst []byte) (n int, err error) {
	return r.Reader.Read(dst)
}

// FromStdCloser wraps io.Closer as Closer.
func FromStdCloser(c io.Closer) Closer { return fromStdCloser{c} }

func (c fromStdCloser) Close(context.Context) error { return c.Closer.Close() }

// FromStdReadCloser wraps io.ReadCloser as ReadCloser.
func FromStdReadCloser(rc io.ReadCloser) ReadCloser {
	return struct {
		Reader
		Closer
	}{FromStdReader(rc), FromStdCloser(rc)}
}

// FromStdReaderAt wraps io.ReaderAt as ReaderAt.
func FromStdReaderAt(r io.ReaderAt) ReaderAt { return fromStdReaderAt{r} }

func (r fromStdReaderAt) ReadAt(_ context.Context, dst []byte, off int64) (n int, err error) {
	return r.ReaderAt.ReadAt(dst, off)
}

// ToStdReader wraps Reader as io.Reader.
func ToStdReader(ctx context.Context, r Reader) io.Reader { return toStdReader{ctx, r} }

func (r toStdReader) Read(dst []byte) (n int, err error) {
	return r.Reader.Read(r.ctx, dst)
}
