// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package traverse

import (
	"fmt"
	"os"
	"sync"
)

// A Reporter receives events from an ongoing traversal. Reporters
// can be passed as options into Traverse, and are used to monitor
// progress of long-running traversals.
type Reporter interface {
	// Init is called when processing is about to begin. Parameter
	// n indicates the number of tasks to be executed by the traversal.
	Init(n int)
	// Complete is called after the traversal has completed.
	Complete()

	// Begin is called when task i is begun.
	Begin(i int)
	// End is called when task i has completed.
	End(i int)
}

// NewSimpleReporter returns a new reporter that prints the number
// of queued, running, and completed tasks to stderr.
func NewSimpleReporter(name string) Reporter {
	return &simpleReporter{name: name}
}

type simpleReporter struct {
	name                  string
	mu                    sync.Mutex
	queued, running, done int
}

func (r simpleReporter) Init(n int) {
	r.mu.Lock()
	r.queued = n
	r.update()
	r.mu.Unlock()
}

func (r simpleReporter) Complete() {
	fmt.Fprintf(os.Stderr, "\n")
}

func (r simpleReporter) Begin(i int) {
	r.mu.Lock()
	r.queued--
	r.running++
	r.update()
	r.mu.Unlock()
}

func (r simpleReporter) End(i int) {
	r.mu.Lock()
	r.running--
	r.done++
	r.update()
	r.mu.Unlock()
}

func (r simpleReporter) update() {
	fmt.Fprintf(os.Stderr, "%s: (queued: %d -> running: %d -> done: %d) \r", r.name, r.queued, r.running, r.done)
}
