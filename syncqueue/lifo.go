// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package syncqueue

import (
	"sync"
)

// LIFO implements a last-in, first-out producer-consumer queue. Thread safe.
type LIFO struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []interface{}
	closed bool
}

// NewFIFO creates an empty FIFO queue.
func NewLIFO() *LIFO {
	q := &LIFO{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Put adds the object in the queue.
func (q *LIFO) Put(v interface{}) {
	q.mu.Lock()
	q.queue = append(q.queue, v)
	q.cond.Signal()
	q.mu.Unlock()
}

// Close informs the queue that no more objects will be added via Put().
func (q *LIFO) Close() {
	q.closed = true
	q.cond.Broadcast()
}

// Get removes the newest object added to the queue.  It blocks the caller if
// the queue is empty.
func (q *LIFO) Get() (interface{}, bool) {
	q.mu.Lock()
	for !q.closed && len(q.queue) == 0 {
		q.cond.Wait()
	}
	var v interface{}
	var ok bool
	if n := len(q.queue); n > 0 {
		v = q.queue[n-1]
		q.queue = q.queue[:n-1]
		ok = true
	}
	q.mu.Unlock()
	return v, ok
}
