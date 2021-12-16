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

	StdReader struct {
		Ctx context.Context
		Reader
	}
	StdSeeker struct {
		Ctx context.Context
		Seeker
	}
	StdReaderAt struct {
		Ctx context.Context
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
func ToStdReader(ctx context.Context, r Reader) io.Reader { return StdReader{ctx, r} }

func (r StdReader) Read(dst []byte) (n int, err error) {
	return r.Reader.Read(r.Ctx, dst)
}

// ToStdSeeker wraps Seeker as io.Seeker.
func ToStdSeeker(ctx context.Context, s Seeker) io.Seeker { return StdSeeker{ctx, s} }

func (r StdSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.Seeker.Seek(r.Ctx, offset, whence)
}

// ToStdReadSeeker wraps ReadSeeker as io.ReadSeeker.
func ToStdReadSeeker(ctx context.Context, rs ReadSeeker) io.ReadSeeker {
	return struct {
		io.Reader
		io.Seeker
	}{ToStdReader(ctx, rs), ToStdSeeker(ctx, rs)}
}

// ToStdReaderAt wraps ReaderAt as io.ReaderAt.
func ToStdReaderAt(ctx context.Context, r ReaderAt) io.ReaderAt { return StdReaderAt{ctx, r} }

func (r StdReaderAt) ReadAt(dst []byte, off int64) (n int, err error) {
	return r.ReaderAt.ReadAt(r.Ctx, dst, off)
}
