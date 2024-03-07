// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
)

// typedConfigure is a type-erased version of func(*Constructor[T]).
type typedConfigure struct {
	configure func(*Constructor[any])
	typ       reflect.Type
}

var (
	globalsMu sync.Mutex
	globals   = make(map[string]typedConfigure)
	defaults  = make(map[string]string)
)

// Register registers a constructor and later invokes the provided
// function whenever a new profile instance is created. Register
// panics if multiple constructors are registered with the same name.
// Constructors should typically be registered in package init
// functions, and the configure function must define at least
// Constructor.New. For example, the following configures a
// constructor with a single parameter, n, which simply returns its
// value.
//
//	config.Register("config/test", func(constr *config.Constructor[int]) {
//		n := constr.Int("n", 32, "the number configured")
//		constr.New = func() (int, error) {
//			return *n, nil
//		}
//		constr.Doc = "a customizable integer"
//	})
func Register[T any](name string, configure func(*Constructor[T])) {
	globalsMu.Lock()
	defer globalsMu.Unlock()
	if _, found := globals[name]; found {
		panic("config.Register: instance with name " + name + " has already been registered")
	}
	globals[name] = typedConfigure{
		func(untyped *Constructor[any]) {
			typed := Constructor[T]{params: untyped.params}
			configure(&typed)
			untyped.Doc = typed.Doc
			untyped.New = func() (any, error) { return typed.New() }
		},
		reflect.TypeOf(new(T)).Elem(),
	}
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
	if _, found := globals[name]; found {
		panic("config.Default: default " + name + " has same name as a global")
	}
	if _, found := globals[instance]; !found {
		if _, found = defaults[instance]; !found {
			panic("config.Default: instance " + instance + " does not exist")
		}
	}
	defaults[name] = instance
}

type (
	// Constructor defines a constructor, as configured by Register.
	// Typically a constructor registers a set of parameters through the
	// flags-like methods provided by Constructor. The value returned by
	// New is configured by these parameters.
	Constructor[T any] struct {
		New    func() (T, error)
		Doc    string
		params map[string]*param
	}
	// Nil is an interface type with no implementations. Constructor[Nil]
	// indicates an instance is created just for its side effects.
	Nil interface{ neverImplemented() }
)

func newConstructor() *Constructor[any] {
	return &Constructor[any]{
		params: make(map[string]*param),
	}
}

// InstanceVar registers a parameter that is satisfied by another
// instance; the method panics if ptr is not a pointer. The default
// value is always an indirection; if it is left empty it is taken as
// the nil value: it remains uninitialized by default.
func (c *Constructor[_]) InstanceVar(ptr interface{}, name string, value string, help string) {
	ptrTyp := reflect.TypeOf(ptr)
	if ptrTyp.Kind() != reflect.Ptr {
		panic(fmt.Sprintf(
			"Instance.InterfaceVar: passed ptr %s is not a pointer",
			ptrTyp,
		))
	}
	param := c.define(name, paramInterface, help)
	param.ifaceptr = ptr
	if value == "nil" {
		value = ""
	}
	if value == "" && !isNilAssignable(ptrTyp.Elem()) {
		// TODO: Consider allowing empty values to mean zero values for types
		// that are not nil-assignable.  We currently do not allow the empty
		// string to be consistent with parsing, as there is no way to set a
		// parameter to an empty value, as we require an identifier.
		panic(fmt.Sprintf(
			"Instance.InterfaceVar: ptr element %s cannot have nil/empty value",
			ptrTyp.Elem(),
		))
	}
	param.ifaceindir = indirect(value)
}

// Int registers an integer parameter with a default value. The returned
// pointer points to its value.
func (c *Constructor[_]) Int(name string, value int, help string) *int {
	p := new(int)
	c.IntVar(p, name, value, help)
	return p
}

// IntVar registers an integer parameter with a default value. The parameter's
// value written to the location pointed to by ptr.
func (c *Constructor[_]) IntVar(ptr *int, name string, value int, help string) {
	*ptr = value
	c.define(name, paramInt, help).intptr = ptr
}

// Float registers floating point parameter with a default value. The returned
// pointer points to its value.
func (c *Constructor[_]) Float(name string, value float64, help string) *float64 {
	p := new(float64)
	c.FloatVar(p, name, value, help)
	return p
}

// FloatVar register a floating point parameter with a default value. The parameter's
// value is written to the provided pointer.
func (c *Constructor[_]) FloatVar(ptr *float64, name string, value float64, help string) {
	*ptr = value
	c.define(name, paramFloat, help).floatptr = ptr
}

// String registers a string parameter with a default value. The returned pointer
// points to its value.
func (c *Constructor[_]) String(name string, value string, help string) *string {
	p := new(string)
	c.StringVar(p, name, value, help)
	return p
}

// StringVar registers a string parameter with a default value. The parameter's
// value written to the location pointed to by ptr.
func (c *Constructor[_]) StringVar(ptr *string, name string, value string, help string) {
	*ptr = value
	c.define(name, paramString, help).strptr = ptr
}

// Bool registers a boolean parameter with a default value. The returned pointer
// points to its value.
func (c *Constructor[_]) Bool(name string, value bool, help string) *bool {
	p := new(bool)
	c.BoolVar(p, name, value, help)
	return p
}

// BoolVar registers a boolean parameter with a default value. The parameter's
// value written to the location pointed to by ptr.
func (c *Constructor[_]) BoolVar(ptr *bool, name string, value bool, help string) {
	*ptr = value
	c.define(name, paramBool, help).boolptr = ptr
}

func (c *Constructor[_]) define(name string, kind int, help string) *param {
	if c.params[name] != nil {
		panic("config: parameter " + name + " already defined")
	}
	p := &param{kind: kind, help: help}
	_, p.file, p.line, _ = runtime.Caller(2)
	c.params[name] = p
	return c.params[name]
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

func isNilAssignable(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Chan:
	case reflect.Func:
	case reflect.Interface:
	case reflect.Map:
	case reflect.Ptr:
	case reflect.Slice:
	case reflect.UnsafePointer:
	default:
		return false
	}
	return true
}
