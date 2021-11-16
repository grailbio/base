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
	"strconv"
	"testing"
	"time"

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

func TestStdInterop(t *testing.T) {
	tests := []struct {
		name    string
		makeErr func() (cleanUp func(), _ error)
		kind    errors.Kind
		target  error
	}{
		{
			"not exist",
			func() (cleanUp func(), _ error) {
				_, err := os.Open("/dev/notexist")
				return func() {}, err
			},
			errors.NotExist,
			os.ErrNotExist,
		},
		{
			"canceled",
			func() (cleanUp func(), _ error) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				<-ctx.Done()
				return func() {}, ctx.Err()
			},
			errors.Canceled,
			context.Canceled,
		},
		{
			"timeout",
			func() (cleanUp func(), _ error) {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
				<-ctx.Done()
				return cancel, ctx.Err()
			},
			errors.Timeout,
			context.DeadlineExceeded,
		},
		{
			"timeout interface",
			func() (cleanUp func(), _ error) {
				return func() {}, apparentTimeoutError{}
			},
			errors.Timeout,
			nil, // Doesn't match a stdlib error.
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cleanUp, err := test.makeErr()
			defer cleanUp()
			for errIdx, err := range []error{
				err,
				errors.E(err),
				errors.E(err, "wrapped", errors.Fatal),
			} {
				t.Run(strconv.Itoa(errIdx), func(t *testing.T) {
					if got, want := errors.Is(test.kind, err), true; got != want {
						t.Errorf("got %v, want %v", got, want)
					}
					if test.target != nil {
						if got, want := goerrors.Is(err, test.target), true; got != want {
							t.Errorf("got %v, want %v", got, want)
						}
					}
					// err should not match wrapped target.
					if got, want := goerrors.Is(err, fmt.Errorf("%w", test.target)), false; got != want {
						t.Errorf("got %v, want %v", got, want)
					}
				})
			}
		})
	}
}

type apparentTimeoutError struct{}

func (e apparentTimeoutError) Error() string { return "timeout" }
func (e apparentTimeoutError) Timeout() bool { return true }

// TestEKindDeterminism ensures that errors.E's Kind detection (based on the
// cause chain of the input error) is deterministic. That is, if the input
// error has multiple causes (according to goerrors.Is), E chooses one
// consistently. User code that handles errors based on Kind will behave
// predictably.
//
// This is a regression test for an issue found while introducing (*Error).Is
// (D65766) which makes it easier for an error chain to match multiple causes.
func TestEKindDeterminism(t *testing.T) {
	const N = 100
	numKind := make(map[errors.Kind]int)
	for i := 0; i < N; i++ {
		// Construct err with a cause chain that matches Canceled due to a
		// Kind and NotExist by wrapping the stdlib error.
		err := errors.E(
			fmt.Errorf("%w",
				errors.E("canceled", errors.Canceled,
					fmt.Errorf("%w", os.ErrNotExist))))
		// Sanity check: err is detected as both targets.
		if got, want := goerrors.Is(err, os.ErrNotExist), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := goerrors.Is(err, context.Canceled), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		numKind[err.(*errors.Error).Kind]++
	}
	// Now, ensure the assigned Kind is Canceled, the lower number.
	if got, want := len(numKind), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := numKind[errors.Canceled], N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
