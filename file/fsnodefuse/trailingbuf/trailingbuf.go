package trailingbuf

import (
	"context"
	"fmt"
	"io"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file/internal/s3bufpool"
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
		// Copying to io.Discard ends up using small chunks from an internal pool. This is a fairly
		// pessimal S3 read size, so since we sometimes read from S3 streams here, we use larger
		// buffers.
		//
		// Note that we may eventually want to use some internal read buffer for all S3 reads, so
		// clients don't accidentally experience bad performance because their application happens
		// to use a pattern of small reads. In that case, this special skip buffer would just add
		// copies, and not help, and we may want to remove it.
		discardBuf := s3bufpool.Get()
		n, err := io.CopyBuffer(
			// Hide io.Discard's io.ReadFrom implementation because CopyBuffer detects that and
			// ignores our buffer.
			struct{ io.Writer }{io.Discard},
			io.LimitReader(ioctx.ToStdReader(ctx, r.pr), skip),
			*discardBuf)
		s3bufpool.Put(discardBuf)
		r.off += n
		if n < skip {
			r.eof = true
			err = io.EOF
		}
		if err != nil {
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
