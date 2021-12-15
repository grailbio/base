package ioctx

import (
	"context"
	"io"
)

type (
	fromStdReader   struct{ io.Reader }
	fromStdCloser   struct{ io.Closer }
	fromStdSeeker   struct{ io.Seeker }
	fromStdReaderAt struct{ io.ReaderAt }

	toStdReader struct {
		ctx context.Context
		Reader
	}
	toStdSeeker struct {
		ctx context.Context
		Seeker
	}
	toStdReaderAt struct {
		ctx context.Context
		ReaderAt
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

// FromStdSeeker wraps io.Seeker as Seeker.
func FromStdSeeker(s io.Seeker) Seeker { return fromStdSeeker{s} }

func (s fromStdSeeker) Seek(_ context.Context, offset int64, whence int) (int64, error) {
	return s.Seeker.Seek(offset, whence)
}

// FromStdReadCloser wraps io.ReadCloser as ReadCloser.
func FromStdReadCloser(rc io.ReadCloser) ReadCloser {
	return struct {
		Reader
		Closer
	}{FromStdReader(rc), FromStdCloser(rc)}
}

// FromStdReadSeeker wraps io.ReadSeeker as ReadSeeker.
func FromStdReadSeeker(rs io.ReadSeeker) ReadSeeker {
	return struct {
		Reader
		Seeker
	}{FromStdReader(rs), FromStdSeeker(rs)}
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

// ToStdSeeker wraps Seeker as io.Seeker.
func ToStdSeeker(ctx context.Context, s Seeker) io.Seeker { return toStdSeeker{ctx, s} }

func (r toStdSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.Seeker.Seek(r.ctx, offset, whence)
}

// ToStdReadSeeker wraps ReadSeeker as io.ReadSeeker.
func ToStdReadSeeker(ctx context.Context, rs ReadSeeker) io.ReadSeeker {
	return struct {
		io.Reader
		io.Seeker
	}{ToStdReader(ctx, rs), ToStdSeeker(ctx, rs)}
}

// ToStdReaderAt wraps ReaderAt as io.ReaderAt.
func ToStdReaderAt(ctx context.Context, r ReaderAt) io.ReaderAt { return toStdReaderAt{ctx, r} }

func (r toStdReaderAt) ReadAt(dst []byte, off int64) (n int, err error) {
	return r.ReaderAt.ReadAt(r.ctx, dst, off)
}
