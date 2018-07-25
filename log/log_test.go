// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package log_test

import (
	"os"
	"testing"

	"github.com/grailbio/base/log"
)

type testOutputter struct {
	level    log.Level
	messages map[log.Level][]string
}

func newTestOutputter(level log.Level) *testOutputter {
	return &testOutputter{level, make(map[log.Level][]string)}
}

func (t *testOutputter) Empty() bool {
	for _, m := range t.messages {
		if len(m) != 0 {
			return false
		}
	}
	return true
}

func (t *testOutputter) Next(level log.Level) string {
	if len(t.messages[level]) == 0 {
		return ""
	}
	var m string
	m, t.messages[level] = t.messages[level][0], t.messages[level][1:]
	return m
}

func (t *testOutputter) Level() log.Level {
	return t.level
}

func (t *testOutputter) Output(calldepth int, level log.Level, s string) error {
	t.messages[level] = append(t.messages[level], s)
	return nil
}

func TestLog(t *testing.T) {
	out := newTestOutputter(log.Info)
	defer log.SetOutputter(log.SetOutputter(out))
	log.Printf("hello %q", "world")
	if got, want := out.Next(log.Info), `hello "world"`; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	log.Error.Print(1, 2, 3)
	if got, want := out.Next(log.Error), "1 2 3"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	log.Debug.Print("x")
	if got, want := out.Next(log.Debug), ""; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if !out.Empty() {
		t.Error("extra messages")
	}
}

func ExampleDefault() {
	log.SetOutput(os.Stdout)
	log.SetFlags(0)
	log.Print("hello, world!")
	log.Error.Print("hello from error")
	log.Debug.Print("invisible")

	// Output:
	// hello, world!
	// hello from error
}
