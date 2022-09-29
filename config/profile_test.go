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
	f float64
}

// paramFields is a convenient structure for testing that has fields of various
// types that we use to test parameter handling.
type paramFields struct {
	c  custom
	p  *custom
	ch chan struct{}
	a  any
}

func init() {
	Register("test/custom", func(inst *Constructor) {
		var c custom
		inst.IntVar(&c.x, "x", -1, "the x value")
		inst.FloatVar(&c.f, "f", 0, "the f value")
		inst.New = func() (interface{}, error) {
			return c, nil
		}
	})

	Default("test/default", "test/custom")

	Register("test/custom-ptr", func(inst *Constructor) {
		var c custom
		inst.IntVar(&c.x, "x", -1, "the x value")
		inst.New = func() (interface{}, error) {
			return &c, nil
		}
	})

	Register("test/1", func(inst *Constructor) {
		var c custom
		inst.InstanceVar(&c, "custom", "test/default", "the custom struct")
		x := inst.Int("x", 123, "the x value")
		inst.New = func() (interface{}, error) {
			return *x + c.x, nil
		}
	})

	Register("test/custom-nil", func(inst *Constructor) {
		inst.New = func() (interface{}, error) {
			return (*custom)(nil), nil
		}
	})

	Default("test/default-custom-nil", "test/custom-nil")

	Register("test/untyped-nil", func(inst *Constructor) {
		inst.New = func() (interface{}, error) {
			return nil, nil
		}
	})

	Default("test/default-untyped-nil", "test/untyped-nil")

	Register("test/params/empty", func(inst *Constructor) {
		var pf paramFields
		inst.InstanceVar(&pf.p, "p", "test/custom-nil", "")
		inst.InstanceVar(&pf.ch, "ch", "", "")
		inst.InstanceVar(&pf.a, "a", "", "")
		inst.New = func() (interface{}, error) {
			return pf, nil
		}
	})

	Register("test/params/nil", func(inst *Constructor) {
		var pf paramFields
		inst.InstanceVar(&pf.p, "p", "nil", "")
		inst.InstanceVar(&pf.ch, "ch", "", "")
		inst.InstanceVar(&pf.a, "a", "nil", "")
		inst.New = func() (interface{}, error) {
			return pf, nil
		}
	})

	Register("test/params/nil-instance", func(inst *Constructor) {
		var pf paramFields
		inst.InstanceVar(&pf.p, "p", "test/custom-nil", "")
		inst.New = func() (interface{}, error) {
			return pf, nil
		}
	})

	Register("test/params/empty-non-nilable-recovered", func(inst *Constructor) {
		var r any
		func() {
			defer func() {
				r = recover()
			}()
			var pf paramFields
			inst.InstanceVar(&pf.c, "c", "", "")
		}()
		inst.New = func() (interface{}, error) {
			return r, nil
		}
	})

	Register("test/params/nil-non-nilable-recovered", func(inst *Constructor) {
		var r any
		func() {
			defer func() {
				r = recover()
			}()
			var pf paramFields
			inst.InstanceVar(&pf.c, "c", "nil", "")
		}()
		inst.New = func() (interface{}, error) {
			return r, nil
		}
	})

	Register("test/chan", func(inst *Constructor) {
		inst.New = func() (interface{}, error) {
			return make(chan struct{}), nil
		}
	})

	Register("test/params/non-nil", func(inst *Constructor) {
		var pf paramFields
		inst.InstanceVar(&pf.c, "c", "test/custom", "")
		inst.InstanceVar(&pf.p, "p", "test/custom-ptr", "")
		inst.InstanceVar(&pf.ch, "ch", "test/chan", "")
		inst.InstanceVar(&pf.a, "a", "test/custom", "")
		inst.New = func() (interface{}, error) {
			return pf, nil
		}
	})
}

func TestProfileDefault(t *testing.T) {
	p := New()
	var x int
	if err := p.Instance("test/1", &x); err != nil {
		t.Fatal(err)
	}
	if got, want := x, 122; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	p = New()
	if err := p.Set("test/custom.x", "-100"); err != nil {
		t.Fatal(err)
	}
	if err := p.Instance("test/1", &x); err != nil {
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

instance testf test/custom (
	f = 1
)
`))
	if err != nil {
		t.Fatal(err)
	}

	var x int
	if err = p.Instance("test/1", &x); err != nil {
		t.Fatal(err)
	}
	if got, want := x, 1000; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if err = p.Instance("testx", &x); err != nil {
		t.Fatal(err)
	}
	if got, want := x, 1099; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	var str string
	err = p.Instance("testx", &str)
	if err == nil || !strings.Contains(err.Error(), "instance \"testx\" of type int is not assignable to provided pointer element type string") {
		t.Error(err)
	}

	var c custom
	if err = p.Instance("testf", &c); err != nil {
		t.Fatal(err)
	}
	if got, want := c.f, 1.; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestNilInstances verifies that we handle nil/empty instances appropriately.
func TestNilInstances(t *testing.T) {
	var (
		mustSet = func(p *Profile, path, value string) {
			t.Helper()
			if err := p.Set(path, value); err != nil {
				t.Error(err)
			}
		}
		mustInstance = func(p *Profile, name string, pa any) {
			t.Helper()
			if err := p.Instance(name, pa); err != nil {
				t.Fatal(err)
			}
		}
		mustEqual = func(got, want any) {
			t.Helper()
			if got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		}
	)

	var (
		p  *Profile
		pc *custom
		a  any
		pf paramFields
	)

	// Verify that top-level instances can be nil.
	p = New()
	mustInstance(p, "test/custom-nil", &pc)
	mustEqual(pc, (*custom)(nil))
	mustInstance(p, "test/default-custom-nil", &pc)
	mustEqual(pc, (*custom)(nil))
	mustInstance(p, "test/untyped-nil", &a)
	mustEqual(a, nil)
	mustInstance(p, "test/default-untyped-nil", &a)
	mustEqual(a, nil)

	// Verify that empty InstanceVar defaults produce nil parameters.
	p = New()
	mustInstance(p, "test/params/empty", &pf)
	mustEqual(pf.p, (*custom)(nil))
	mustEqual(pf.ch, (chan struct{})(nil))
	mustEqual(pf.a, nil)

	// Verify that nil InstanceVar defaults produce nil parameters.
	p = New()
	mustInstance(p, "test/params/nil", &pf)
	mustEqual(pf.p, (*custom)(nil))
	mustEqual(pf.ch, (chan struct{})(nil))
	mustEqual(pf.a, nil)

	// Verify that an InstanceVar default instance whose value is nil produces
	// a nil parameter.
	p = New()
	mustInstance(p, "test/params/nil-instance", &pf)
	mustEqual(pf.p, (*custom)(nil))

	// Verify that InstanceVar panics setting an empty value default for an
	// element type that cannot be assigned nil.
	p = New()
	// Set c to a valid instance, so that the invalid instance error does not
	// obscure the recovered panic value.
	mustSet(p, "test/params/empty-non-nilable-recovered.c", "test/custom")
	mustInstance(p, "test/params/empty-non-nilable-recovered", &a)
	if a == nil {
		t.Error("expected non-nil-assignable empty default instance to panic")
	}

	// Verify that InstanceVar panics setting a nil value default for an
	// element type that cannot be assigned nil.
	p = New()
	// Set c to a valid instance, so that the invalid instance error does not
	// obscure the recovered panic value.
	mustSet(p, "test/params/nil-non-nilable-recovered.c", "test/custom")
	mustInstance(p, "test/params/nil-non-nilable-recovered", &a)
	if a == nil {
		t.Error("expected non-nil-assignable nil default instance to panic")
	}

	// Verify that a non-nil-assignable parameter set to an empty instance is
	// invalid.
	p = New()
	mustSet(p, "test/params/non-nil.c", "")
	if err := p.Instance("test/params/non-nil", &pf); err == nil {
		t.Error("non-nil-assignable set to empty instance should return non-nil error")
	}

	// Verify that a non-nil-assignable parameter set to nil is invalid.
	p = New()
	mustSet(p, "test/params/non-nil.c", "nil")
	if err := p.Instance("test/params/non-nil", &pf); err == nil {
		t.Error("non-nil-assignable set to nil should return non-nil error")
	}

	// Verify that nil-assignable parameters can be set to empty, resulting in
	// nil parameter values.
	p = New()
	mustSet(p, "test/params/non-nil.p", "")
	mustSet(p, "test/params/non-nil.ch", "")
	mustSet(p, "test/params/non-nil.a", "")
	mustInstance(p, "test/params/non-nil", &pf)
	mustEqual(pf.p, (*custom)(nil))
	mustEqual(pf.ch, (chan struct{})(nil))
	mustEqual(pf.a, nil)

	// Verify that nil-assignable parameters can be set to nil, resulting in
	// nil parameter values.
	p = New()
	mustSet(p, "test/params/non-nil.p", "nil")
	mustSet(p, "test/params/non-nil.ch", "nil")
	mustSet(p, "test/params/non-nil.a", "nil")
	mustInstance(p, "test/params/non-nil", &pf)
	mustEqual(pf.p, (*custom)(nil))
	mustEqual(pf.ch, (chan struct{})(nil))
	mustEqual(pf.a, nil)

	// Verify that a nil-assignable parameter can be set to an instance whose
	// value is nil, resulting in a nil parameter value.
	p = New()
	mustSet(p, "test/params/non-nil.p", "test/custom-nil")
	mustInstance(p, "test/params/non-nil", &pf)
	mustEqual(pf.p, (*custom)(nil))

	// Verify that top-level instances cannot be set to empty.
	p = New()
	if err := p.Set("test/custom", ""); err == nil {
		t.Error("top-level instance set to empty should return non-nil error")
	}

	// Verify that top-level instances cannot be set to nil.
	p = New()
	if err := p.Set("test/custom", ""); err == nil {
		t.Error("top-level instance set to nil should return non-nil error")
	}
}

func TestSetGet(t *testing.T) {
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

instance testy test/1

`))
	if err != nil {
		t.Fatal(err)
	}

	var (
		mustGet = func(k, want string) {
			t.Helper()
			got, ok := p.Get(k)
			if !ok {
				t.Fatalf("key %v not found", k)
			}
			if got != want {
				t.Fatalf("key %v: got %v, want %v", k, got, want)
			}
		}
		mustSet = func(k, v string) {
			t.Helper()
			if err := p.Set(k, v); err != nil {
				t.Fatalf("set %v %v: %v", k, v, err)
			}
		}
	)

	mustGet("testy", "test/1")
	mustGet("test/1.x", "1")
	mustGet("testx.x", "100")
	mustGet("testx.custom", "test/custom")
	mustGet("testx.custom.x", "999")
	mustGet("testy.x", "1")

	mustSet("testx.custom.x", "1900")
	mustGet("testx.custom.x", "1900")
	mustSet("testx.custom.x", "-1900")
	mustGet("testx.custom.x", "-1900")
	mustSet("testx.custom.f", "3.14")
	mustGet("testx.custom.f", "3.14")
	mustSet("testx.custom.f", "-3.14")
	mustGet("testx.custom.f", "-3.14")

	mustSet("testy", "testx")
	mustGet("testy.x", "100")

}

// TestInstanceNames verifies that InstanceNames returns the correct set of
// instance names.
func TestInstanceNames(t *testing.T) {
	p := New()
	names := p.InstanceNames()
	// Because global instances can be added from anywhere, we only verify that
	// the returned names contains the instances added by this file.
	for _, name := range []string{
		"test/1",
		"test/custom",
		"test/default",
	} {
		if _, ok := names[name]; !ok {
			t.Errorf("missing instance name=%v", name)
		}
	}
}
