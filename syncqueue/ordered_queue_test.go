package syncqueue_test

import (
	"fmt"
	"testing"

	"github.com/grailbio/base/syncqueue"
	"github.com/stretchr/testify/assert"
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
	resultChan := make(chan bool, 1)

	q := syncqueue.NewOrderedQueue(2)
	q.Insert(1, "one")
	q.Insert(0, "zero")

	go func() {
		q.Insert(2, "two")
		resultChan <- true
		close(resultChan)
	}()

	checkNext(t, q, "zero", true)
	result := <-resultChan
	assert.True(t, result, "Expected insert(2, two) to complete after removing an item from the queue")
	q.Close(nil)

	checkNext(t, q, "one", true)
	checkNext(t, q, "two", true)
	checkNext(t, q, nil, false)
}

func TestInsertBlockWithoutNextAvailable(t *testing.T) {
	resultChan := make(chan bool, 1)

	q := syncqueue.NewOrderedQueue(2)
	q.Insert(1, "one")

	go func() {
		q.Insert(2, "two")
		resultChan <- true
		close(resultChan)
	}()

	q.Insert(0, "zero")
	checkNext(t, q, "zero", true)

	// Wait until insert two finishes
	result := <-resultChan
	assert.True(t, result, "Expected insert(2, two) to complete after removing an item from the queue")
	q.Close(nil)

	checkNext(t, q, "one", true)
	checkNext(t, q, "two", true)
	checkNext(t, q, nil, false)
}

func TestNextBlockWhenEmpty(t *testing.T) {
	resultChan := make(chan bool, 1)

	q := syncqueue.NewOrderedQueue(2)
	go func() {
		checkNext(t, q, "zero", true)
		resultChan <- true
		close(resultChan)
	}()

	// Insert zero and then wait until Next returns
	q.Insert(0, "zero")
	result := <-resultChan
	assert.True(t, result, "Expected Next() to complete after inserting zero")

	q.Close(nil)
	checkNext(t, q, nil, false)
}

func TestInsertGetsError(t *testing.T) {
	errors := make(chan error, 1)

	q := syncqueue.NewOrderedQueue(1)
	q.Insert(0, "zero")

	go func() {
		errors <- q.Insert(1, "one")
		close(errors)
	}()

	// Close q with an error.
	q.Close(fmt.Errorf("Foo error"))

	// Wait for Insert to return with an error, and verify the value of the error.
	e := <-errors
	assert.Equal(t, "Foo error", e.Error())
}

func TestNextGetsError(t *testing.T) {
	errorChan := make(chan error, 1)

	q := syncqueue.NewOrderedQueue(1)
	go func() {
		_, _, err := q.Next()
		errorChan <- err
		close(errorChan)
	}()

	q.Close(fmt.Errorf("Foo error"))
	err := <-errorChan
	assert.Equal(t, "Foo error", err.Error())
}
