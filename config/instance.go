// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"reflect"
	"runtime"
	"sync"
)

var (
	globalsMu sync.Mutex
	globals   = make(map[string]func(*Instance))
	defaults  = make(map[string]string)
)

// Register registers a global instance and later invokes the
// provided function whenever a new profile instance is created.
// Register panics if multiple instances are registered with the same
// name. Instances should typically be registered in package init
// functions, and the configure function must define at least
// Instance.New. For example, the following configures an instance
// with a single parameter, n, which simply returns its value.
//
//	config.Register("config/test", func(inst *config.Instance) {
//		n := inst.Int("n", 32, "the number configured")
//		inst.New = func() (interface{}, error) {
//			return *n, nil
//		}
//		inst.Doc = "a customizable integer"
//	})
func Register(name string, configure func(*Instance)) {
	globalsMu.Lock()
	defer globalsMu.Unlock()
	if globals[name] != nil {
		panic("config.Register: instance with name " + name + " has already been registered")
	}
	globals[name] = configure
}

// Default declares a new derived instance. It is a convenience
// function used to provide a default implementation among multiple
// choices, and is equivalent to the the profile directive
//
//	instance name instance
//
// Default panics if name is already the name of an instance, or if
// the specified parent instance does not exist.
func Default(name, instance string) {
	globalsMu.Lock()
	defer globalsMu.Unlock()
	if globals[name] != nil {
		panic("config.Default: default " + name + " has same name as a global")
	}
	if globals[instance] == nil {
		panic("config.Default: instance " + instance + " does not exist")
	}
	defaults[name] = instance
}

// Instance is a global instance, as configured by Register.
// Typically an instance registers a set of parameters through the
// flags-like methods provided by Instance. The value returned by New
// is configured by these parameters.
type Instance struct {
	// New instantiates the value provided by this instance, and
	// configured by the flags registered.
	//
	// TODO(marius): consider making this an interface{} so that we can
	// inspect the return type (typechecking can be done in Register)
	// and allow us to perform better type checking.
	New func() (interface{}, error)

	// Doc is a string describing the instance.
	Doc string

	params map[string]*param
}

func newInstance() *Instance {
	return &Instance{
		params: make(map[string]*param),
	}
}

// InstanceVar registers a parameter that is satisfied by another
// instance; the method panics if ptr is not a pointer. The default
// value is always an indirection; if it is left empty it is taken as
// the nil value: it remains uninitialized by default.
func (inst *Instance) InstanceVar(ptr interface{}, name string, value string, help string) {
	if reflect.TypeOf(ptr).Kind() != reflect.Ptr {
		panic("Instance.InterfaceVar: passed interface is not a pointer")
	}
	param := inst.define(name, paramInterface, help)
	param.ifaceptr = ptr
	param.ifaceindir = indirect(value)
}

// Int registers an integer parameter with a default value. The returned
// pointer points to its value.
func (inst *Instance) Int(name string, value int, help string) *int {
	p := new(int)
	inst.IntVar(p, name, value, help)
	return p
}

// IntVar registers an integer parameter with a default value. The parameter's
// value written to the location pointed to by ptr.
func (inst *Instance) IntVar(ptr *int, name string, value int, help string) {
	*ptr = value
	inst.define(name, paramInt, help).intptr = ptr
}

// Float registers floating point parameter with a default value. The returned
// pointer points to its value.
func (inst *Instance) Float(name string, value float64, help string) *float64 {
	p := new(float64)
	inst.FloatVar(p, name, value, help)
	return p
}

// FloatVar register a floating point parameter with a default value. The parameter's
// value is written to the provided pointer.
func (inst *Instance) FloatVar(ptr *float64, name string, value float64, help string) {
	*ptr = value
	inst.define(name, paramFloat, help).floatptr = ptr
}

// String registers a string parameter with a default value. The returned pointer
// points to its value.
func (inst *Instance) String(name string, value string, help string) *string {
	p := new(string)
	inst.StringVar(p, name, value, help)
	return p
}

// StringVar registers a string parameter with a default value. The parameter's
// value written to the location pointed to by ptr.
func (inst *Instance) StringVar(ptr *string, name string, value string, help string) {
	*ptr = value
	inst.define(name, paramString, help).strptr = ptr
}

// Bool registers a boolean parameter with a default value. The returned pointer
// points to its value.
func (inst *Instance) Bool(name string, value bool, help string) *bool {
	p := new(bool)
	inst.BoolVar(p, name, value, help)
	return p
}

// BoolVar registers a boolean parameter with a default value. The parameter's
// value written to the location pointed to by ptr.
func (inst *Instance) BoolVar(ptr *bool, name string, value bool, help string) {
	*ptr = value
	inst.define(name, paramBool, help).boolptr = ptr
}

func (inst *Instance) define(name string, kind int, help string) *param {
	if inst.params[name] != nil {
		panic("config: parameter " + name + " already defined")
	}
	p := &param{kind: kind, help: help}
	_, p.file, p.line, _ = runtime.Caller(2)
	inst.params[name] = p
	return inst.params[name]
}

const (
	paramInterface = iota
	paramInt
	paramFloat
	paramString
	paramBool
)

type param struct {
	kind int
	help string

	file string
	line int

	intptr     *int
	floatptr   *float64
	ifaceptr   interface{}
	ifaceindir indirect
	strptr     *string
	boolptr    *bool
}

func (p *param) Interface() interface{} {
	switch p.kind {
	case paramInterface:
		return reflect.ValueOf(p.ifaceptr).Elem().Interface()
	case paramInt:
		return *p.intptr
	case paramFloat:
		return *p.floatptr
	case paramString:
		return *p.strptr
	case paramBool:
		return *p.boolptr
	default:
		panic(p.kind)
	}
}
