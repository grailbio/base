// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package errors_test

import (
	"bytes"
	"context"
	"encoding/gob"
	goerrors "errors"
	"fmt"
	"os"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/base/errors"
)

// generate random errors and test encoding, etc.  (fuzz)

func TestError(t *testing.T) {
	_, err := os.Open("/dev/notexist")
	e1 := errors.E(errors.NotExist, "opening file", err)
	if got, want := e1.Error(), "opening file: resource does not exist: open /dev/notexist: no such file or directory"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	e2 := errors.E(err)
	if got, want := e2.Error(), "resource does not exist: open /dev/notexist: no such file or directory"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	for _, e := range []error{e1, e2} {
		if !errors.Is(errors.NotExist, e) {
			t.Errorf("error %v should be NotExist", e)
		}
	}
}

func TestErrorChaining(t *testing.T) {
	_, err := os.Open("/dev/notexist")
	err = errors.E("failed to open file", err)
	err = errors.E(errors.Retriable, "cannot proceed", err)
	if got, want := err.Error(), "cannot proceed: resource does not exist (retriable):\n\tfailed to open file: open /dev/notexist: no such file or directory"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

type temporaryError string

func (t temporaryError) Error() string   { return string(t) }
func (t temporaryError) Temporary() bool { return true }

func TestIsTemporary(t *testing.T) {
	for _, c := range []struct {
		err       error
		temporary bool
	}{
		{errors.E(context.DeadlineExceeded), true},
		{errors.E(context.Canceled), false},
		{goerrors.New("no idea"), false},
		{temporaryError(""), true},
		{errors.E(temporaryError(""), errors.NotExist), true},
		{errors.E(errors.Temporary, "failed to open socket"), true},
		{errors.E("no idea"), false},
		{errors.E(errors.Fatal, "fatal error"), false},
		{errors.E(errors.Retriable, "this one you can retry"), true},
		{errors.E(fmt.Errorf("test")), false},
	} {
		if got, want := errors.IsTemporary(c.err), c.temporary; got != want {
			t.Errorf("error %v: got %v, want %v", c.err, got, want)
		}
		if c.temporary {
			continue
		}
		if !errors.IsTemporary(errors.E(c.err, errors.Temporary)) {
			t.Errorf("error %v: temporary conversion failed", c.err)
		}
	}
}

func TestGobEncoding(t *testing.T) {
	_, err := os.Open("/dev/notexist")
	err = errors.E("failed to open file", err)
	err = errors.E(errors.Fatal, "cannot proceed", err)

	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(errors.Recover(err)); err != nil {
		t.Fatal(err)
	}
	e2 := new(errors.Error)
	if err := gob.NewDecoder(&b).Decode(e2); err != nil {
		t.Fatal(err)
	}
	if !errors.Match(err, e2) {
		t.Errorf("error %v does not match %v", err, e2)
	}
}

func TestGobEncodingFuzz(t *testing.T) {
	fz := fuzz.New().NilChance(0).Funcs(
		func(e *errors.Error, c fuzz.Continue) {
			c.Fuzz(&e.Kind)
			c.Fuzz(&e.Severity)
			c.Fuzz(&e.Message)
			if c.Float32() < 0.8 {
				var e2 errors.Error
				c.Fuzz(&e2)
				e.Err = &e2
			}
		},
	)

	const N = 1000
	for i := 0; i < N; i++ {
		var err errors.Error
		fz.Fuzz(&err)
		var b bytes.Buffer
		if err := gob.NewEncoder(&b).Encode(errors.Recover(&err)); err != nil {
			t.Fatal(err)
		}
		e2 := new(errors.Error)
		if err := gob.NewDecoder(&b).Decode(e2); err != nil {
			t.Fatal(err)
		}
		if !errors.Match(&err, e2) {
			t.Errorf("error %v does not match %v", &err, e2)
		}
	}
}

func TestMessage(t *testing.T) {
	for _, c := range []struct {
		err     error
		message string
	}{
		{errors.E("hello"), "hello"},
		{errors.E("hello", "world"), "hello world"},
	} {
		if got, want := c.err.Error(), c.message; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
