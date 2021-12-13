package trailingbuf

import (
	"context"
	"fmt"
	"io"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/base/morebufio"
	"github.com/grailbio/base/must"
)

// ErrTooFarBehind is returned if a read goes too far behind the current position.
// It's set as a cause (which callers can unwrap) on some errors returned by ReadAt.
var ErrTooFarBehind = errors.New("trailbuf: read too far behind")

type ReaderAt struct {
	// semaphore guards all subsequent fields. It's used to serialize operations.
	semaphore chan struct{}
	// r is the data source.
	pr morebufio.PeekBackReader
	// off is the number of bytes we've read from r.
	off int64
	// eof is true after r returns io.EOF.
	eof bool
}

// New creates a ReaderAt that can respond to arbitrary reads as long as they're close
// to the current position. trailSize controls the max distance (controlling buffer space usage).
// Reads too far behind the current position return an error with cause ErrTooFarBehind.
func New(r ioctx.Reader, trailSize int) *ReaderAt {
	return &ReaderAt{
		semaphore: make(chan struct{}, 1),
		pr:        morebufio.NewPeekBackReader(r, trailSize),
	}
}

// ReadAt implements io.ReaderAt.
func (r *ReaderAt) ReadAt(ctx context.Context, dst []byte, off int64) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, errors.E(errors.Invalid, "trailbuf: negative offset")
	}

	select {
	case r.semaphore <- struct{}{}:
		defer func() { <-r.semaphore }()
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	var nDst int
	// Try to peek backwards from r.off, if requested.
	if back := r.off - off; back > 0 {
		peekBack := r.pr.PeekBack()
		if back > int64(len(peekBack)) {
			return nDst, errors.E(errors.Invalid, ErrTooFarBehind,
				fmt.Sprintf("trailbuf: read would seek backwards: request %d(%d), current pos %d(-%d)",
					off, len(dst), r.off, len(peekBack)))
		}
		peekUsed := copy(dst, peekBack[len(peekBack)-int(back):])
		dst = dst[peekUsed:]
		nDst += int(peekUsed)
		off += int64(peekUsed)
	}
	// If we're already at EOF (so there's not enough data to reach off), or len(dst)
	// is small enough (off + len(dst) < r.off), we exit early.
	// Otherwise, we've advanced the request offset up to the current cursor and need to
	// read more of the underlying stream.
	if r.eof {
		return nDst, io.EOF
	}
	if len(dst) == 0 {
		return nDst, nil
	}
	must.Truef(off >= r.off, "%d, %d", off, r.off)

	// Skip forward in r.pr, if necessary.
	if skip := off - r.off; skip > 0 {
		n, err := io.CopyN(io.Discard, ioctx.ToStdReader(ctx, r.pr), skip)
		r.off += n
		if err != nil {
			if err == io.EOF {
				r.eof = true
			}
			return nDst, err
		}
	}

	// Complete the read.
	n, err := io.ReadFull(ioctx.ToStdReader(ctx, r.pr), dst)
	r.off += int64(n)
	nDst += n
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		err = io.EOF
		r.eof = true
	}
	return nDst, err
}

// Size returns the final number of bytes obtained from the underlying stream, if we've already
// found EOF, else _, false.
func (r *ReaderAt) Size(ctx context.Context) (size int64, known bool, err error) {
	select {
	case r.semaphore <- struct{}{}:
		defer func() { <-r.semaphore }()
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}

	if r.eof {
		return r.off, true, nil
	}
	return 0, false, nil
}
