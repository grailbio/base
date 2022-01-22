// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package once

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/grailbio/base/traverse"
)

func TestTaskOnceConcurrency(t *testing.T) {
	const (
		N      = 10
		resets = 2
	)
	var (
		o     Task
		count int32
	)
	for r := 0; r < resets; r++ {
		err := traverse.Each(N, func(_ int) error {
			return o.Do(func() error {
				atomic.AddInt32(&count, 1)
				return nil
			})
		})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := atomic.LoadInt32(&count), int32(r+1); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		o.Reset()
	}
}

func TestMapOnceConcurrency(t *testing.T) {
	const N = 10
	var (
		once  Map
		count uint32
	)
	err := traverse.Each(N, func(jobIdx int) error {
		return once.Do(123, func() error {
			atomic.AddUint32(&count, 1)
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := count, uint32(1); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestTaskOnceError(t *testing.T) {
	var (
		once     Map
		expected = errors.New("expected error")
	)
	err := once.Do(123, func() error { return expected })
	if got, want := err, expected; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	err = once.Do(123, func() error { panic("should not be called") })
	if got, want := err, expected; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	err = once.Do(124, func() error { return nil })
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
}
