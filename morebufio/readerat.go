package morebufio

import (
	"context"
	"io"
	"sync"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/ioctx"
)

type readerAt struct {
	r ioctx.ReaderAt
	// mu guards updates to all subsequent fields. It's not held during reads.
	mu sync.Mutex
	// seekerInUse is true while waiting for operations (like reads) on seeker.
	seekerInUse bool
	seeker      *readSeeker
}

// NewReaderAt constructs a buffered ReaderAt. While ReaderAt allows arbitrary concurrent reads,
// this implementation has only one buffer (for simplicity), so reads will only be usefully buffered
// if reads are serial and generally contiguous (just like a plain ioctx.Reader). Concurrent reads
// will be "passed through" to the underlying ReaderAt, just without buffering.
func NewReaderAtSize(r ioctx.ReaderAt, size int) ioctx.ReaderAt {
	return &readerAt{
		r:      r,
		seeker: NewReadSeekerSize(&readerAtSeeker{r: r}, size),
	}
}

// ReadAt implements ioctx.ReaderAt.
func (r *readerAt) ReadAt(ctx context.Context, dst []byte, off int64) (int, error) {
	acquired := r.tryAcquireSeeker()
	if !acquired {
		return r.r.ReadAt(ctx, dst, off)
	}
	defer r.releaseSeeker()
	if _, err := r.seeker.Seek(ctx, off, io.SeekStart); err != nil {
		return 0, errors.E(err, "seeking for ReadAt")
	}
	return r.seeker.Read(ctx, dst)
}

func (r *readerAt) tryAcquireSeeker() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seekerInUse {
		return false
	}
	r.seekerInUse = true
	return true
}

func (r *readerAt) releaseSeeker() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.seekerInUse {
		panic("release of unacquired seeker")
	}
	r.seekerInUse = false
}

// readerAtSeeker is a simple ioctx.ReadSeeker that only supports seeking by io.SeekCurrent,
// which is all readSeeker requires.
type readerAtSeeker struct {
	r   ioctx.ReaderAt
	pos int64
}

func (r *readerAtSeeker) Read(ctx context.Context, p []byte) (int, error) {
	n, err := r.r.ReadAt(ctx, p, r.pos)
	r.pos += int64(n)
	return n, err
}

func (r *readerAtSeeker) Seek(ctx context.Context, request int64, whence int) (int64, error) {
	if whence == io.SeekCurrent {
		r.pos += request
		return r.pos, nil
	}
	// Pretend the end position is zero. readSeeker requests this at initialization but we
	// won't use it after that.
	r.pos = 0
	return r.pos, nil
}
