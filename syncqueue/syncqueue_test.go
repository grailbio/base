// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package syncqueue_test

import (
	"fmt"
	"testing"

	"github.com/grailbio/base/syncqueue"
	"github.com/stretchr/testify/require"
)

func ExampleLIFO() {
	q := syncqueue.NewLIFO()
	q.Put("item0")
	q.Put("item1")
	q.Close()
	v0, ok := q.Get()
	fmt.Println("Item 0:", v0.(string), ok)
	v1, ok := q.Get()
	fmt.Println("Item 1:", v1.(string), ok)
	v2, ok := q.Get()
	fmt.Println("Item 2:", v2, ok)
	// Output:
	// Item 0: item1 true
	// Item 1: item0 true
	// Item 2: <nil> false
}

func TestLIFOWithThreads(t *testing.T) {
	q := syncqueue.NewLIFO()
	ch := make(chan string, 3)

	// Check if "ch" has any data.
	chanEmpty := func() bool {
		select {
		case <-ch:
			return false
		default:
			return true
		}
	}

	go func() {
		for {
			val, ok := q.Get()
			if !ok {
				break
			}
			ch <- val.(string)
		}
	}()
	s := []string{}
	q.Put("item0")
	q.Put("item1")
	s = append(s, <-ch, <-ch)
	require.True(t, chanEmpty())

	q.Put("item2")
	s = append(s, <-ch)
	require.True(t, chanEmpty())

	require.Equal(t, []string{"item1", "item0", "item2"}, s)
}
