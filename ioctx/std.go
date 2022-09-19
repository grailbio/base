package ioctx

import (
	"context"
	"io"
)

type (
	fromStdReader   struct{ io.Reader }
	fromStdWriter   struct{ io.Writer }
	fromStdCloser   struct{ io.Closer }
	fromStdSeeker   struct{ io.Seeker }
	fromStdReaderAt struct{ io.ReaderAt }
	fromStdWriterAt struct{ io.WriterAt }

	StdReader struct {
		Ctx context.Context
		Reader
	}
	StdWriter struct {
		Ctx context.Context
		Writer
	}
	StdCloser struct {
		Ctx context.Context
		Closer
	}
	StdSeeker struct {
		Ctx context.Context
		Seeker
	}
	StdReadCloser struct {
		Ctx context.Context
		ReadCloser
	}
	StdReaderAt struct {
		Ctx context.Context
		ReaderAt
	}
	StdWriterAt struct {
		Ctx context.Context
		WriterAt
	}
)

// FromStdReader wraps io.Reader as Reader.
func FromStdReader(r io.Reader) Reader { return fromStdReader{r} }

func (r fromStdReader) Read(_ context.Context, dst []byte) (n int, err error) {
	return r.Reader.Read(dst)
}

// FromStdWriter wraps io.Writer as Writer.
func FromStdWriter(w io.Writer) Writer { return fromStdWriter{w} }

func (w fromStdWriter) Write(_ context.Context, p []byte) (n int, err error) {
	return w.Writer.Write(p)
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

// ToStdWriter wraps Writer as io.Writer.
func ToStdWriter(ctx context.Context, w Writer) io.Writer { return StdWriter{ctx, w} }

func (w StdWriter) Write(p []byte) (n int, err error) {
	return w.Writer.Write(w.Ctx, p)
}

// ToStdCloser wraps Closer as io.Closer.
func ToStdCloser(ctx context.Context, c Closer) io.Closer { return StdCloser{ctx, c} }

func (c StdCloser) Close() error {
	return c.Closer.Close(c.Ctx)
}

// ToStdSeeker wraps Seeker as io.Seeker.
func ToStdSeeker(ctx context.Context, s Seeker) io.Seeker { return StdSeeker{ctx, s} }

func (r StdSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.Seeker.Seek(r.Ctx, offset, whence)
}

// ToStdReadCloser wraps ReadCloser as io.ReadCloser.
func ToStdReadCloser(ctx context.Context, rc ReadCloser) io.ReadCloser {
	return struct {
		io.Reader
		io.Closer
	}{ToStdReader(ctx, rc), ToStdCloser(ctx, rc)}
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

// ToStdWriterAt wraps WriterAt as io.WriterAt.
func ToStdWriterAt(ctx context.Context, w WriterAt) io.WriterAt { return StdWriterAt{ctx, w} }

func (w StdWriterAt) WriteAt(dst []byte, off int64) (n int, err error) {
	return w.WriterAt.WriteAt(w.Ctx, dst, off)
}
