// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package file

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/grailbio/base/errors"
)

// A testReader simulates various scenarios based on parameters.
// Fills p (fully) with random data for the first "times-1" calls.
// Fills p (partially) with random data for the "times"th call.
// and then returns "err" if its not nil or EOF.
type testReader struct {
	data         []byte
	index, times int
	err          error

	nRead int64
}

func (t *testReader) Read(p []byte) (n int, err error) {
	if t.index == t.times {
		if t.err != nil {
			return 0, t.err

		}
		return 0, io.EOF
	}
	if t.index == t.times-1 {
		buf := make([]byte, rand.Intn(len(p)))
		rand.Read(buf)
		n, err = copy(p, buf), nil
	} else {
		n, err = rand.Read(p)
	}

	t.data = append(t.data, p[0:n]...)
	t.nRead += int64(n)
	t.index++

	time.Sleep(time.Nanosecond)
	return
}

// A testWriter simulates various scenarios based on parameters.
// Writes the buffer data into "data" for the first "times" calls
// and then returns "err" if its not nil.
// If short is set to true, writes partially.
type testWriter struct {
	data         []byte
	index, times int
	err          error
	short        bool

	nWrite int64
}

func (t *testWriter) Write(p []byte) (n int, err error) {
	if t.index == t.times {
		if t.err != nil {
			return 0, t.err
		}
		return 0, nil
	}
	n, err = len(p), nil
	if t.short {
		n, err = rand.Intn(len(p)), nil
	}

	t.data = append(t.data, p[0:n]...)
	t.nWrite += int64(n)
	t.index++

	time.Sleep(time.Nanosecond)
	return
}

func TestCopy(t *testing.T) {
	r, w := &testReader{times: 5}, &testWriter{times: 5, err: nil}
	n, err := Copy(context.Background(), w, r)
	if want, got := r.data[:r.nRead], w.data[:n]; !bytes.Equal(want, got) {
		t.Errorf("copy size: got %v, want %v", len(got), len(want))
	}
	if err != nil {
		t.Errorf("unexpected: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	r, w = &testReader{times: -1}, &testWriter{times: -1, err: nil}
	n, err = Copy(ctx, w, r)
	cancel()
	// Wait some more time to make sure the copy go routine didn't keep copying.
	time.Sleep(10 * time.Millisecond)
	if want, got := context.DeadlineExceeded, err; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if want, got := r.data[:w.nWrite], w.data[:n]; !bytes.Equal(want, got) {
		t.Errorf("copy size: got %v, want %v", len(got), len(want))
	}

	custom := errors.E(errors.Integrity, "custom error")

	r, w = &testReader{times: 2, err: custom}, &testWriter{times: 2, err: nil}
	n, err = Copy(context.Background(), w, r)
	if want, got := r.data[:r.nRead], w.data[:n]; !bytes.Equal(want, got) {
		t.Errorf("copy size: got %v, want %v", len(got), len(want))
	}
	if want, got := errors.E("file.Copy", custom), err; !reflect.DeepEqual(want, got) {
		t.Errorf("got %v, want %v", got, want)
	}

	r, w = &testReader{times: 10}, &testWriter{times: 3, err: custom}
	n, err = Copy(context.Background(), w, r)
	if want, got := r.data[:w.nWrite], w.data[:n]; !bytes.Equal(want, got) {
		t.Errorf("copy size: got %v, want %v", len(got), len(want))
	}
	if want, got := errors.E("file.Copy", custom), err; !reflect.DeepEqual(want, got) {
		t.Errorf("got %v, want %v", got, want)
	}

	r, w = &testReader{times: 1}, &testWriter{times: 1, short: true}
	n, err = Copy(context.Background(), w, r)
	if want, got := r.data[:w.nWrite], w.data[:n]; !bytes.Equal(want, got) {
		t.Errorf("copy size: got %v, want %v", len(got), len(want))
	}
	if want, got := errors.E("file.Copy", io.ErrShortWrite), err; !reflect.DeepEqual(want, got) {
		t.Errorf("got %v, want %v", got, want)
	}
}
