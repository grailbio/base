package semaphore

import (
	"context"
)

type (
	// S is a concurrency limiter. Holders of a resource unit (represented by
	// *Item) also get temporary ownership of a memory buffer.
	// TODO: Consider implementing some ordering or prioritization. For example, while copying
	// many files in multiple chunks each, maybe we prefer to use available concurrency to complete
	// all the chunks of one file rather than a couple of chunks of many, so that if there's an
	// error we have made some useful progress.
	S    struct{ c chan *Item }
	Item struct {
		// bufSize describes the eventual size of buf, since it's allocated lazily.
		bufSize int
		// buf is scratch space that the (temporary) owner of this item can use while holding
		// this item. That is, they must not use it after release.
		buf []byte
		// releaseTo is the collection that Item will be returned to, when the user is done.
		// It's set only when the *Item is in use, for best-effort detection of incorrect API usage.
		releaseTo S
	}
)

// New constructs S. Total possible buffer allocation is capacity*bufSize, but it's allocated lazily
// per-Item.
func New(capacity int, bufSize int) S {
	s := S{make(chan *Item, capacity)}
	for i := 0; i < capacity; i++ {
		s.c <- &Item{bufSize: bufSize}
	}
	return s
}

// Acquire waits for semaphore capacity, respecting context cancellation.
// Returns exactly one of item or error. If non-nil, Item must be released.
//
// Intended usage:
//   item, err := sema.Acquire(ctx)
//   if err != nil {
//   	return /* ..., */ err
//   )
//   defer item.Release()
//   doMyThing(item.Buf())
func (s S) Acquire(ctx context.Context) (*Item, error) {
	select {
	case item := <-s.c:
		item.releaseTo = s
		return item, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Cap returns the capacity (that was passed to New).
func (s S) Cap() int { return cap(s.c) }

// Buf borrows this item's buffer. Caller must not use it after releasing the item.
func (i *Item) Buf() []byte {
	if len(i.buf) < i.bufSize {
		i.buf = make([]byte, i.bufSize)
	}
	return i.buf
}

// Release returns capacity to the semaphore. It must be called exactly once after acquisition.
func (i *Item) Release() {
	if i == nil {
		panic("usage error: Release after failed Acquire")
	}
	if i.releaseTo.c == nil {
		panic("usage error: multiple Release")
	}
	releaseTo := i.releaseTo
	i.releaseTo = S{}
	// Note: We must modify i before sending it to the channel, to avoid a race.
	releaseTo.c <- i
}
