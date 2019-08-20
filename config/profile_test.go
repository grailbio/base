// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"strings"
	"testing"
)

type custom struct {
	x int
}

func init() {
	Register("test/custom", func(inst *Instance) {
		var c custom
		inst.IntVar(&c.x, "x", -1, "the x value")
		inst.New = func() (interface{}, error) {
			return c, nil
		}
	})

	Default("test/default", "test/custom")

	Register("test/1", func(inst *Instance) {
		var c custom
		inst.InstanceVar(&c, "custom", "test/default", "the custom struct")
		x := inst.Int("x", 123, "the x value")
		inst.New = func() (interface{}, error) {
			return *x + c.x, nil
		}
	})
}

func TestProfileDefault(t *testing.T) {
	p := New()
	var x int
	if err := p.Get("test/1", &x); err != nil {
		t.Fatal(err)
	}
	if got, want := x, 122; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	p = New()
	p.Set("test/custom.x", "-100")
	if err := p.Get("test/1", &x); err != nil {
		t.Fatal(err)
	}
	if got, want := x, 23; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestProfile(t *testing.T) {
	p := New()
	err := p.Parse(strings.NewReader(`
param test/custom (
	x = 999
)

param test/1 (
	custom = test/custom
	x = 1
)

instance testx test/1 (
	x = 100
)

`))
	if err != nil {
		t.Fatal(err)
	}

	var x int
	if err := p.Get("test/1", &x); err != nil {
		t.Fatal(err)
	}
	if got, want := x, 1000; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if err := p.Get("testx", &x); err != nil {
		t.Fatal(err)
	}
	if got, want := x, 1099; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	var str string
	err = p.Get("testx", &str)
	if err == nil || !strings.Contains(err.Error(), "testx: instance type int not assignable to provided type *string") {
		t.Error(err)
	}
}
