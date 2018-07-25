package readmatcher

import (
	"context"
	stderrors "errors"
	"sync"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file/fsnodefuse/trailingbuf"
	"github.com/grailbio/base/file/internal/kernel"
	"github.com/grailbio/base/ioctx"
)

type (
	// TODO: Avoid somewhat hidden internal dependency on kernel.MaxReadAhead.
	readerAt struct {
		offsetReader   func(int64) ioctx.ReadCloser
		softMaxReaders int

		// mu guards the fields below. It's held while looking up a reader, but not during reads.
		// TODO: Consider making this RWMutex.
		mu sync.Mutex
		// clock counts reader usages, increasing monotonically. Reader creation and usage is
		// "timestamped" according to this clock, letting us prune least-recently-used.
		clock int64
		// TODO: More efficient data structure.
		readers readers
	}
	// readers is a collection of backend readers. It's ordered by createdAt:
	//   readers[i].createdAt < readers[j].createdAt iff i < j.
	// Elements in the middle may be removed; then we just shift the tail forward by 1.
	readers []*reader
	reader  struct {
		// These fields are set at creation and never mutated.
		ioctx.ReaderAt
		ioctx.Closer

		// These fields are accessed only while holding the the parent readerAt's lock.
		maxPos     int64
		inUse      int64
		lastUsedAt int64
		createdAt  int64
	}
)

const defaultMaxReaders = 1024

var (
	_ ioctx.ReaderAt = (*readerAt)(nil)
	_ ioctx.Closer   = (*readerAt)(nil)
)

type Opt func(*readerAt)

func SoftMaxReaders(n int) Opt { return func(r *readerAt) { r.softMaxReaders = n } }

// New returns a ReaderAt that "multiplexes" incoming reads onto one of a collection of "backend"
// readers. It matches read to backend based on last read position; a reader is selected if its last
// request ended near where the new read starts.
//
// It is intended for use with biofs+S3. S3 readers have high initialization costs vs.
// subsequently reading bytes, because that is S3's performance characteristic. ReaderAt maps
// incoming reads to a backend S3 reader that may be able to efficiently serve it. Otherwise, it
// opens a new reader. Our intention is that this will adapt to non-S3-aware clients' read
// patterns (small reads). S3-aware clients can always choose to read big chunks to avoid
// performance worst-cases. But, the Linux kernel limits FUSE read requests to 128 KiB, and we
// can't feasibly change that, so we adapt.
//
// To performantly handle Linux kernel readahead requests, the matching algorithm allows
// out-of-order positions within a small window (see trailingbuf).
//
// offsetReader opens a reader into the underlying file.
func New(offsetReader func(int64) ioctx.ReadCloser, opts ...Opt) interface {
	ioctx.ReaderAt
	ioctx.Closer
} {
	r := readerAt{offsetReader: offsetReader, softMaxReaders: defaultMaxReaders}
	for _, opt := range opts {
		opt(&r)
	}
	return &r
}

func (m *readerAt) ReadAt(ctx context.Context, dst []byte, off int64) (int, error) {
	var minCreatedAt int64
	for {
		r := m.acquire(off, minCreatedAt)
		n, err := r.ReadAt(ctx, dst, off)
		m.release(r, off+int64(n))
		if err != nil && stderrors.Is(err, trailingbuf.ErrTooFarBehind) {
			minCreatedAt = r.createdAt + 1
			continue
		}
		return n, err
	}
}

func (m *readerAt) acquire(off int64, minCreatedAt int64) *reader {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.readers {
		if r.createdAt < minCreatedAt {
			continue
		}
		if r.maxPos-kernel.MaxReadAhead <= off && off <= r.maxPos+kernel.MaxReadAhead {
			r.inUse++
			r.lastUsedAt = m.clock
			m.clock++
			return r
		}
	}
	m.lockedGC()
	rc := m.offsetReader(off)
	r := &reader{
		ReaderAt:   trailingbuf.New(rc, off, kernel.MaxReadAhead),
		Closer:     rc,
		maxPos:     off,
		inUse:      1,
		lastUsedAt: m.clock,
		createdAt:  m.clock,
	}
	m.clock++
	m.readers.add(r)
	return r
}

func (m *readerAt) release(r *reader, newPos int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if newPos > r.maxPos {
		r.maxPos = newPos
	}
	r.inUse--
	m.lockedGC()
}

func (m *readerAt) lockedGC() {
	for len(m.readers) > m.softMaxReaders {
		i, ok := m.readers.idleLeastRecentlyUsedIndex()
		if !ok {
			return
		}
		m.readers.remove(i)
	}
}

func (m *readerAt) Close(ctx context.Context) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rc := range m.readers {
		errors.CleanUpCtx(ctx, rc.Close, &err)
	}
	m.readers = nil
	return
}

func (rs *readers) add(r *reader) {
	*rs = append(*rs, r)
}

func (rs *readers) remove(i int) {
	*rs = append((*rs)[:i], (*rs)[i+1:]...)
}

func (rs *readers) idleLeastRecentlyUsedIndex() (int, bool) {
	minIdx := -1
	for i, r := range *rs {
		if r.inUse > 0 {
			continue
		}
		if minIdx < 0 || r.lastUsedAt < (*rs)[minIdx].lastUsedAt {
			minIdx = i
		}
	}
	if minIdx < 0 {
		return -1, false
	}
	return minIdx, true
}
