package morebufio

import (
	"context"

	"github.com/grailbio/base/ioctx"
)

// PeekBackReader is a Reader augmented with a function to "peek" backwards at the data that
// was already passed. Peeking does not change the current stream position (that is, PeekBack has
// no effect on the next Read).
type PeekBackReader interface {
	ioctx.Reader
	// PeekBack returns a fixed "window" of the data that Read has already returned, ending at
	// the current Read position. It may be smaller at the start until enough data has been read,
	// but after that it's constant size. PeekBack is allowed after Read returns EOF.
	// The returned slice aliases the internal buffer and is invalidated by the next Read.
	PeekBack() []byte
}

type peekBackReader struct {
	r   ioctx.Reader
	buf []byte
}

// NewPeekBackReader returns a PeekBackReader. It doesn't have a "forward" buffer so small
// PeekBackReader.Read operations cause small r.Read operations.
func NewPeekBackReader(r ioctx.Reader, peekBackSize int) PeekBackReader {
	return &peekBackReader{r, make([]byte, 0, peekBackSize)}
}

func (p *peekBackReader) Read(ctx context.Context, dst []byte) (int, error) {
	nRead, err := p.r.Read(ctx, dst)
	dst = dst[:nRead]
	// First, grow the peek buf until cap, since it starts empty.
	if grow := cap(p.buf) - len(p.buf); grow > 0 {
		if len(dst) < grow {
			grow = len(dst)
		}
		p.buf = append(p.buf, dst[:grow]...)
		dst = dst[grow:]
	}
	if len(dst) == 0 {
		return nRead, err
	}
	// Shift data if any part of the peek buf is still valid.
	updateTail := p.buf
	if len(dst) < len(p.buf) {
		n := copy(p.buf, p.buf[len(dst):])
		updateTail = p.buf[n:]
	}
	_ = copy(updateTail, dst[len(dst)-len(updateTail):])
	return nRead, err
}

func (p *peekBackReader) PeekBack() []byte { return p.buf }
