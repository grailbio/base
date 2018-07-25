package syncqueue_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/grailbio/base/syncqueue"
)

func checkNext(t *testing.T, q *syncqueue.OrderedQueue, value interface{}, ok bool) {
	actualValue, actualOk, actualErr := q.Next()
	assert.Equal(t, nil, actualErr)
	assert.Equal(t, ok, actualOk)
	assert.Equal(t, value, actualValue)
}

func TestBasic(t *testing.T) {
	testCases := []struct {
		indices []int
		entries []string
	}{
		{
			[]int{0, 1, 2},
			[]string{"zero", "one", "two"},
		},
		{
			[]int{2, 1, 0},
			[]string{"two", "one", "zero"},
		},
		{
			[]int{0, 2, 1},
			[]string{"zero", "two", "one"},
		},
	}

	for _, testCase := range testCases {
		q := syncqueue.NewOrderedQueue(10)
		for i, entry := range testCase.entries {
			q.Insert(testCase.indices[i], entry)
		}
		q.Close(nil)

		t.Logf("testCase: %v", testCase.entries)
		checkNext(t, q, "zero", true)
		checkNext(t, q, "one", true)
		checkNext(t, q, "two", true)
		checkNext(t, q, nil, false)
	}
}

func TestNoBlockWhenNextPresent(t *testing.T) {
	q := syncqueue.NewOrderedQueue(2)
	q.Insert(0, "zero")
	q.Insert(1, "one")
	q.Close(nil)

	checkNext(t, q, "zero", true)
	checkNext(t, q, "one", true)
	checkNext(t, q, nil, false)
}

func TestNoBlockWhenInsertNext(t *testing.T) {
	q := syncqueue.NewOrderedQueue(2)
	q.Insert(1, "one")
	q.Insert(0, "zero")
	q.Close(nil)

	checkNext(t, q, "zero", true)
	checkNext(t, q, "one", true)
	checkNext(t, q, nil, false)
}

func TestInsertBlockWithNextAvailable(t *testing.T) {
	cond := sync.NewCond(&sync.Mutex{})

	q := syncqueue.NewOrderedQueue(2)
	q.Insert(1, "one")
	q.Insert(0, "zero")

	insertedTwo := false
	go func() {
		q.Insert(2, "two")
		insertedTwo = true
		cond.Signal()
	}()

	assert.False(t, insertedTwo, "Expected insert(2, two) to block until there is space in the queue")
	checkNext(t, q, "zero", true)
	cond.L.Lock()
	for !insertedTwo {
		cond.Wait()
	}
	assert.True(t, insertedTwo, "Expected insert(2, two) to complete after removing an item from the queue")
	cond.L.Unlock()

	q.Close(nil)

	checkNext(t, q, "one", true)
	checkNext(t, q, "two", true)
	checkNext(t, q, nil, false)
}

func TestInsertBlockWithoutNextAvailable(t *testing.T) {
	cond := sync.NewCond(&sync.Mutex{})

	q := syncqueue.NewOrderedQueue(2)
	q.Insert(1, "one")

	insertedTwo := false
	go func() {
		q.Insert(2, "two")
		insertedTwo = true
		cond.Signal()
	}()

	assert.False(t, insertedTwo, "Expected insert(2, two) to block until there is space in the queue")
	q.Insert(0, "zero")
	checkNext(t, q, "zero", true)

	// Wait until insert two finishes
	cond.L.Lock()
	for !insertedTwo {
		cond.Wait()
	}
	assert.True(t, insertedTwo, "Expected insert(2, two) to complete after removing an item from the queue")
	cond.L.Unlock()

	q.Close(nil)

	checkNext(t, q, "one", true)
	checkNext(t, q, "two", true)
	checkNext(t, q, nil, false)
}

func TestNextBlockWhenEmpty(t *testing.T) {
	cond := sync.NewCond(&sync.Mutex{})

	q := syncqueue.NewOrderedQueue(2)
	gotZero := false
	go func() {
		checkNext(t, q, "zero", true)
		gotZero = true
		cond.Signal()
	}()

	assert.False(t, gotZero, "Expected Next block until there is something in the queue")

	// Insert zero and then wait until Next returns
	q.Insert(0, "zero")
	cond.L.Lock()
	for !gotZero {
		cond.Wait()
	}
	assert.True(t, gotZero, "Expected Next() to complete after inserting zero")
	cond.L.Unlock()

	q.Close(nil)
	checkNext(t, q, nil, false)
}

func TestInsertGetsError(t *testing.T) {
	cond := sync.NewCond(&sync.Mutex{})

	q := syncqueue.NewOrderedQueue(1)
	q.Insert(0, "zero")

	var insertError error
	go func() {
		insertError = q.Insert(1, "one")
		cond.Signal()
	}()

	assert.Nil(t, insertError, "Expected insert(1, one) to be nil until there is an error")
	q.Close(fmt.Errorf("Foo error"))

	cond.L.Lock()
	for insertError == nil {
		cond.Wait()
	}
	assert.Equal(t, "Foo error", insertError.Error())
	cond.L.Unlock()
}

func TestNextGetsError(t *testing.T) {
	cond := sync.NewCond(&sync.Mutex{})

	q := syncqueue.NewOrderedQueue(1)
	var nextError error
	go func() {
		_, _, nextError = q.Next()
		cond.Signal()
	}()

	assert.Nil(t, nextError, "Expected nextError to be nil until there is an error")
	q.Close(fmt.Errorf("Foo error"))

	cond.L.Lock()
	for nextError == nil {
		cond.Wait()
	}
	assert.Equal(t, "Foo error", nextError.Error())
	cond.L.Unlock()
}
