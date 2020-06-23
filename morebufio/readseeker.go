package morebufio

import (
	"io"
)

type readSeeker struct {
	r io.ReadSeeker
	// buf is the buffer, resized as necessary after reading from r.
	buf []byte
	// off is the caller's current offset into the buffer. buf[off:] is unread.
	off int

	// filePos is the caller's current position r's stream. This can be different from r's position,
	// for example when there's unread data in buf. Equals -1 when uninitialized.
	filePos int64
	// fileEnd is the offset of the end of r, uused for efficiently seeking within r. Equals -1 when
	// uninitialized.
	fileEnd int64
}

var _ io.ReadSeeker = (*readSeeker)(nil)

// minBufferSize equals bufio.minBufferSize.
const minBufferSize = 16

// NewReadSeekerSize returns a buffered io.ReadSeeker whose buffer has at least the specified size.
// If r is already a readSeeker with sufficient size, returns r.
func NewReadSeekerSize(r io.ReadSeeker, size int) *readSeeker {
	if b, ok := r.(*readSeeker); ok && len(b.buf) >= size {
		return b
	}
	if size < minBufferSize {
		size = minBufferSize
	}
	return &readSeeker{r, make([]byte, 0, size), 0, -1, -1}
}

// Read implements io.Reader.
func (b *readSeeker) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := b.initFilePos(); err != nil {
		return 0, err
	}
	var err error
	if b.off == len(b.buf) {
		b.buf = b.buf[:cap(b.buf)]
		var n int
		n, err = b.r.Read(b.buf)
		b.buf, b.off = b.buf[:n], 0
	}
	n := copy(p, b.buf[b.off:])
	b.off += n
	b.filePos += int64(n)
	return n, err
}

// Seek implements io.Seeker.
func (b *readSeeker) Seek(request int64, whence int) (int64, error) {
	if err := b.initFilePos(); err != nil {
		return 0, err
	}
	var diff int64
	switch whence {
	case io.SeekStart:
		diff = request - b.filePos
	case io.SeekCurrent:
		diff = request
	case io.SeekEnd:
		diff = b.fileEnd + request - b.filePos
	default:
		panic(whence)
	}
	if -int64(b.off) <= diff && diff <= int64(len(b.buf)-b.off) {
		// Seek within buffer without changing file position.
		b.off += int(diff)
		b.filePos += diff
		return b.filePos, nil
	}
	// Discard the buffer and seek the underlying reader.
	diff -= int64(len(b.buf) - b.off)
	b.buf, b.off = b.buf[:0], 0
	var err error
	b.filePos, err = b.r.Seek(diff, io.SeekCurrent)
	return b.filePos, err
}

// initFilePos idempotently initializes filePos and fileEnd.
func (b *readSeeker) initFilePos() error {
	if b.filePos >= 0 && b.fileEnd >= 0 {
		return nil
	}
	var err error
	b.filePos, err = b.r.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	b.fileEnd, err = b.r.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	_, err = b.r.Seek(b.filePos, io.SeekStart)
	return err
}
