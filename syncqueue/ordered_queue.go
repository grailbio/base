package syncqueue

import (
	"fmt"
	"sync"
)

// OrderedQueue is a queue that orders entries on their way out.  An
// inserter enqueues each entry with an index, and when the receiver
// dequeues an entry, the entries arrive in the sequential order of
// their indices.  An OrderedQueue has a maximum size; if the queue
// contains the next entry, the queue will accept up to maxSize
// entries.  If the queue does not yet contain the next entry, the
// queue will block an insert that would make the size equal to
// maxSize unless the new entry is the next entry to be dequeued.
type OrderedQueue struct {
	next    int
	maxSize int
	pending map[int]interface{}
	cond    *sync.Cond
	closed  bool
	err     error
}

// Create a new OrderedQueue with size maxSize.
func NewOrderedQueue(maxSize int) *OrderedQueue {
	if maxSize < 1 {
		panic("OrderedQueue must have length at least 1")
	}
	return &OrderedQueue{
		next:    0,
		maxSize: maxSize,
		pending: make(map[int]interface{}),
		cond:    sync.NewCond(&sync.Mutex{}),
		closed:  false,
	}
}

// Insert an entry into the queue with the specified sequence index.
// Insert blocks if the insert would make the queue full and the new
// entry is not the one next one in the output sequence.  The sequence
// index should start at zero.
func (q *OrderedQueue) Insert(index int, value interface{}) error {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	_, haveNext := q.pending[q.next]
	for q.err == nil && ((haveNext && len(q.pending) == q.maxSize) || (!haveNext && index != q.next && len(q.pending) == q.maxSize-1)) {
		q.cond.Wait()
		_, haveNext = q.pending[q.next]
	}
	if q.err != nil {
		return q.err
	}
	if q.closed {
		panic("closed OrderedQueue before Insert finished")
	}

	q.pending[index] = value
	if index == q.next {
		q.cond.Broadcast()
	}
	return nil
}

// Close the ordered queue.  In the normal case, err should be nil,
// and Close tells the OrderedQueue that all calls to Insert have
// completed.  The user may not call Insert() after calling Close().
// The user may close the OrderedQueue prematurely with a non-nil err
// while some calls to Insert() and/or Next() are blocking; in this
// case those calls that are blocking will return with err.
func (q *OrderedQueue) Close(err error) error {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.err == nil {
		q.err = err
	}
	q.closed = true
	q.cond.Broadcast()
	return q.err
}

// Next returns true and the next entry in sequence if the next entry
// is available.  Next blocks if the next entry is not yet present.
// Next returns (nil, false) if there are no more entries, and the
// queue is closed.
func (q *OrderedQueue) Next() (value interface{}, ok bool, err error) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	value, found := q.pending[q.next]
	for q.err == nil && (!found && !q.closed) {
		q.cond.Wait()
		value, found = q.pending[q.next]
	}
	if q.err != nil {
		return nil, false, q.err
	}
	if q.closed && len(q.pending) == 0 {
		return nil, false, nil
	}
	if q.closed && !found {
		panic(fmt.Sprintf("OrderedQueue is closed, but entry %d is not present", q.next))
	}

	delete(q.pending, q.next)
	q.next++
	q.cond.Broadcast()
	return value, true, nil
}
