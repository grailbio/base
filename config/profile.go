// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package config is used to configure software systems. A
// configuration managed by package config is called a profile. The
// binary that loads a profile declares a set of named, global
// objects through the APIs in this package. A profile configures
// these objects (objects may depend on each other, forming a DAG)
// and lets the user retrieve configured objects through its API.
//
// The semantics of profiles provide the kind of flexibility that is
// often required in operational contexts. Profiles define a
// principled overriding so that a base configuration can be extended
// by the user, either by composing multiple configuration or by
// editing the configuration through a command-line integration.
// Profiles may also derive multiple instances from the same base
// instance in order to provide small variations on instance
// configuration. Profiles define a concrete syntax so that they may
// be stored (e.g., centrally) or transmitted over a network
// connection (e.g., to bootstrap a remote binary with a particular
// configuration). Profiles are also self-documenting in the manner
// of Go's flag package. Profiles are resolved lazily, and thus
// maintain configuration for unknown instances, so long as these are
// never retrieved. This permits a single profile to be reused across
// many binaries without concern for compatibility.
//
// Profile syntax
//
// A profile contains a set of clauses, or directives. Each clause
// either declares a new instance or configures an existing instance.
// Clauses are interpreted in order, top-to-bottom, and later
// configurations override earlier configurations. These semantics
// accommodate for "overlays", where for example a user profile is
// loaded after a base profile to provide customization. Within
// GRAIL, a base profile is declared in the standard package
// github.com/grailbio/base/grail, which also loads a user
// profile from $HOME/grail/profile.
//
// A parameter is set by the directive param. For example, the
// following sets the parallelism parameter on the instance bigslice
// to 1024:
//
//	param bigslice parallelism = 1024
//
// The values supported by profiles are: integers, strings, booleans, floats,
// and indirections (naming other instances). The following shows an example
// of each:
//
//	param bigslice load-factor = 0.8
//	param bigmachine/ec2system username = "marius"
//	param bigmachine/ec2system on-demand = false
//	param s3 retries = 8
//
// As a shortcut, parameters for the same instance may be grouped
// together. For example, the two parameters on the instance
// bigmachine/ec2system may be grouped together as follows:
//
//	param bigmachine/ec2system (
//		username = "marius"
//		on-demand = false
//	)
//
// Instances may refer to each other by name. The following
// configures the aws/ticket instance to use a particular ticket path
// and region; it then configures bigmachine/ec2system to use this
// AWS session.
//
//	param aws/ticket (
//		path = "eng/dev/aws"
//		region = "us-west-2"
//	)
//
//	param bigmachine/ec2system aws = aws/ticket
//
// Profiles may also define new instances with different configurations.
// This is done via the instance directive. For example, if we wanted to
// declare a new bigmachine/ec2system that used on-demand instances
// instead of spot instances, we could define a profile as follows:
//
//	instance bigmachine/ec2ondemand bigmachine/ec2system
//
//	param bigmachine/ec2ondemand on-demand = false
//
// Since it is common to declare an instance and configure it, the
// profile syntax provides an affordance for combining the two,
// also through grouping. The above is equivalent to:
//
//	instance bigmachine/ec2ondemand bigmachine/ec2system (
//		on-demand = false
//		username = "marius-ondemand"
//		// (any other configuration to be changed from the base)
//	)
//
// New instances may depend on any instance. For example, the above
// may be further customized as follows.
//
//	instance bigmachine/ec2ondemand-anonymous bigmachine/ec2ondemand (
//		username = "anonymous"
//	)
//
// Customization through flags
//
// Profile parameters may be adjusted via command-line flags. Profile
// provides utility methods to register flags and interpret them. See
// the appropriate methods for more details. Any parameter may be
// set through the provided command-line flags by specifying the path
// to the parameter. As an example, the following invocations customize
// aspects of the above profile.
//
//	# Override the ticket path and the default ec2system username.
//	# -set flags are interpreted in order, and the following is equivalent
//	# to the clauses
//	# 	param aws/ticket path = "eng/prod/aws"
//	#	param bigmachine/ec2system username = "anonymous"
//	$ program -set aws/ticket.path=eng/prod/aws -set bigmachine/ec2system.username=anonymous
//
//	# User the aws/env instance instead of aws/ticket, as above.
//	# The type of a flag is interpreted based on underlying type, so
//	# the following is equivalent to the clause
//	# 	param bigmachine/ec2system aws = aws/env
//	$ program -set bigmachine/ec2system.aws=aws/env
//
// Default profile
//
// Package config also defines a default profile and a set of package-level
// methods that operate on this profile. Most users should make use only
// of the default profile. This package also exports an http handler on the
// path /debug/profile on the default (global) ServeMux, which returns the
// global profile in parseable form.
package config

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

func init() {
	http.HandleFunc("/debug/profile", func(w http.ResponseWriter, r *http.Request) {
		Application().PrintTo(w)
	})
}

// Profile stores a set of parameters and configures instances based
// on these. It is the central data structure of this package as
// detailed in the package docs. Each Profile instance maintains its
// own set of instances. Most users should use the package-level
// methods that operate on the default profile.
type Profile struct {
	// The following are used by the flag registration and
	// handling mechanism.
	flagDefaultPath string
	flagPaths       []string
	flagParams      []string
	flagDump        bool

	globals map[string]*Constructor

	mu        sync.Mutex
	instances instances
	cached    map[string]interface{}
}

// New creates and returns a new profile, installing all currently
// registered global objects. Global objects registered after a call
// to New are not reflected in the returned profile.
func New() *Profile {
	p := &Profile{
		globals:   make(map[string]*Constructor),
		instances: make(instances),
		cached:    make(map[string]interface{}),
	}

	globalsMu.Lock()
	for name, configure := range globals {
		p.globals[name] = newConstructor()
		configure(p.globals[name])
	}
	globalsMu.Unlock()

	// Make a shadow instance for each global instance. This helps keep
	// the downstream code simple. We also populate any defaults
	// provided by the configured instances, so that printing the
	// profile shows the true, global (and re-createable) state of the
	// profile.
	for name, global := range p.globals {
		inst := &instance{name: name, params: make(map[string]interface{})}
		for pname, param := range global.params {
			// Special case for interface params: use their indirections
			// instead of their value; this is always how they are satisfied
			// in practice.
			if param.kind == paramInterface {
				inst.params[pname] = def{param.ifaceindir}
			} else {
				inst.params[pname] = def{param.Interface()}
			}
		}
		p.instances[name] = inst
	}

	// Populate defaults as empty instance declarations, effectively
	// redirecting the instance and making it overridable, etc.
	globalsMu.Lock()
	for name, parent := range defaults {
		p.instances[name] = &instance{name: name, parent: parent}
	}
	globalsMu.Unlock()

	return p
}

// Set sets the value of the parameter at the provided path to the
// provided value, which is intepreted according to the type of the
// parameter at that path. Set returns an error if the parameter does
// not exist or if the value cannot be parsed into the expected type.
// The path is a set of identifiers separated by dots ("."). Paths may
// traverse multiple indirections.
func (p *Profile) Set(path string, value string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Special case: toplevel instance assignment.
	elems := strings.Split(path, ".")
	if len(elems) == 1 {
		p.instances[elems[0]] = &instance{
			name:   elems[0],
			parent: value,
		}
		return nil
	}

	// Otherwise infer the type and parse it accordingly.
	inst := p.instances[elems[0]]
	if inst == nil {
		return fmt.Errorf("%s: path not found: instance not found", path)
	}
	for i := 1; i < len(elems)-1; i++ {
		var v interface{}
		for {
			var ok bool
			v, ok = inst.params[elems[i]]
			if ok {
				break
			}
			if inst.parent == "" || p.instances[inst.parent] == nil {
				return fmt.Errorf("%s: path not found: instance not found: %s", path, strings.Join(elems[:i], "."))
			}
			inst = p.instances[inst.parent]
		}
		v = unwrap(v)
		indir, ok := v.(indirect)
		if !ok {
			return fmt.Errorf("%s: path not found: %s is not an instance", path, strings.Join(elems[:i], "."))
		}
		inst = p.instances[string(indir)]
		if inst == nil {
			return fmt.Errorf("%s: path not found: instance not found: %s", path, strings.Join(elems[:i], "."))
		}
	}

	name := elems[len(elems)-1]
	for {
		if _, ok := inst.params[name]; ok {
			break
		}
		if inst.parent == "" || p.instances[inst.parent] == nil {
			return fmt.Errorf("%s: no such parameter", path)
		}
		inst = p.instances[inst.parent]
	}

	switch v := unwrap(inst.params[name]); v.(type) {
	case indirect:
		// TODO(marius): validate that it's a good identifier?
		inst.params[name] = indirect(value)
	case string:
		inst.params[name] = value
	case bool:
		v, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("param %s is a bool, but could not parse %s into bool: %v", path, value, err)
		}
		inst.params[name] = v
	case int:
		v, err := strconv.ParseInt(value, 0, 64)
		if err != nil {
			return fmt.Errorf("param %s is an int, but could not parse %s into int: %v", path, value, err)
		}
		inst.params[name] = int(v)
	default:
		panic(fmt.Sprintf("%T", v))
	}
	return nil
}

// Get returns the value of the configured parameter at the provided
// dot-separated path.
func (p *Profile) Get(path string) (value string, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var (
		elems = strings.Split(path, ".")
		inst  = p.instances[elems[0]]
	)
	if inst == nil {
		return "", false
	}
	// Special case: toplevels are "set" only if they are inherited.
	// We return only the first level of inheritance.
	if len(elems) == 1 {
		return inst.parent, inst.parent != ""
	}

	for i := 1; i < len(elems)-1; i++ {
		elem := elems[i]
		for inst != nil && inst.params[elem] == nil {
			inst = p.instances[inst.parent]
		}
		if inst == nil {
			return "", false
		}
		v := unwrap(inst.params[elem])
		indir, ok := v.(indirect)
		if !ok {
			return "", false
		}
		inst = p.instances[string(indir)]
		if inst == nil {
			return "", false
		}
	}

	for elem := elems[len(elems)-1]; inst != nil; inst = p.instances[inst.parent] {
		if v, ok := inst.params[elem]; ok {
			return fmt.Sprintf("%#v", unwrap(v)), true
		}
	}
	return "", false
}

// Merge merges the instance parameters in profile q into p,
// so that parameters defined in q override those in p.
func (p *Profile) Merge(q *Profile) {
	defer lock(p, q)()
	for _, inst := range q.instances {
		p.instances.Merge(inst)
	}
}

// Parse parses a profile from the provided reader into p. On
// success, the instances defined by the profile in src are merged into
// profile p. If the reader implements
//
//	Name() string
//
// then the result of calling Name is used as a filename to provide
// positional information in errors.
func (p *Profile) Parse(r io.Reader) error {
	insts, err := parse(r)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, inst := range insts {
		p.instances.Merge(inst)
	}
	return nil
}

// Instance retrieves the named instance from this profile into the
// pointer ptr. All of its parameters are fully resolved and the
// underlying global object is instantiated according to the desired
// parameterization. Instance panics if ptr is not a pointer type. If the
// type of the instance cannot be assigned to the value pointed to by
// ptr, an error is returned. Since such errors may occur
// transitively (e.g., the type of an instance required by another
// instance may be wrong), the source location of the type mismatch
// is included in the error to help with debugging. Instances are
// cached and are only initialized the first time they are requested.
//
// If ptr is nil, the instance is created without populating the pointer.
func (p *Profile) Instance(name string, ptr interface{}) error {
	var ptrv reflect.Value
	if ptr != nil {
		ptrv = reflect.ValueOf(ptr)
		if ptrv.Kind() != reflect.Ptr {
			panic("profile.Get: not a pointer")
		}
	}
	_, file, line, _ := runtime.Caller(1)
	p.mu.Lock()
	err := p.getLocked(name, ptrv, file, line)
	p.mu.Unlock()
	return err
}

func (p *Profile) PrintTo(w io.Writer) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	instances := p.sorted()
	for _, inst := range instances {
		if len(inst.params) == 0 && inst.parent == "" {
			continue
		}
		// Try to populate parameter docs if we can.
		var docs map[string]string
		if global := p.globals[inst.name]; global != nil {
			docs = make(map[string]string)
			for name, param := range global.params {
				docs[name] = param.help
			}
		}
		if _, err := fmt.Fprintln(w, inst.SyntaxString(docs)); err != nil {
			return err
		}
	}
	return nil
}

func (p *Profile) getLocked(name string, ptr reflect.Value, file string, line int) error {
	if v, ok := p.cached[name]; ok {
		return assign(name, v, ptr, file, line)
	}
	inst := p.instances[name]
	if inst == nil {
		return fmt.Errorf("no instance named %q", name)
	}

	resolved := make(map[string]interface{})
	for {
		for k, v := range inst.params {
			if _, ok := resolved[k]; !ok {
				resolved[k] = v
			}
		}
		if inst.parent == "" {
			break
		}
		parent := p.instances[inst.parent]
		if parent == nil {
			return fmt.Errorf("no such instance: %q", inst.parent)
		}
		inst = parent
	}

	if p.globals[inst.name] == nil {
		return fmt.Errorf("missing global instance: %q", inst.name)
	}
	// Even though we have a configured instance in globals, we create
	// a new one to reduce the changes that multiple instances clobber
	// each other.
	globalsMu.Lock()
	configure := globals[inst.name]
	globalsMu.Unlock()
	instance := newConstructor()
	configure(instance)

	for pname, param := range instance.params {
		val, ok := resolved[pname]
		if !ok {
			continue
		}
		// Skip defaults except for paramInterface since these need to be resolved.
		if _, ok := val.(def); ok && param.kind != paramInterface {
			continue
		}
		val = unwrap(val)
		if indir, ok := val.(indirect); ok {
			if param.kind != paramInterface {
				return fmt.Errorf("resolving %s.%s: cannot indirect parameters of type %T", name, pname, val)
			}
			if indir == "" { // nil: skip
				continue
			}
			if err := p.getLocked(string(indir), reflect.ValueOf(param.ifaceptr), param.file, param.line); err != nil {
				return err
			}
			continue
		}

		switch param.kind {
		case paramInterface:
			var (
				dst = reflect.ValueOf(param.ifaceptr).Elem()
				src = reflect.ValueOf(val)
			)
			// TODO: include embedded fields, etc?
			if !src.Type().AssignableTo(dst.Type()) {
				return fmt.Errorf("%s.%s: cannot assign value of type %s to type %s", name, pname, src.Type(), dst.Type())
			}
			dst.Set(src)
		case paramInt:
			ival, ok := val.(int)
			if !ok {
				return fmt.Errorf("%s.%s: wrong parameter type: expected int, got %T", name, pname, val)
			}
			*param.intptr = ival
		case paramFloat:
			fval, ok := val.(float64)
			if !ok {
				return fmt.Errorf("%s.%s: wrong parameter type: expected float64, got %T", name, pname, val)
			}
			*param.floatptr = fval
		case paramString:
			sval, ok := val.(string)
			if !ok {
				return fmt.Errorf("%s.%s: wrong parameter type: expected string, got %T", name, pname, val)
			}
			*param.strptr = sval
		case paramBool:
			bval, ok := val.(bool)
			if !ok {
				return fmt.Errorf("%s.%s: wrong parameter type: expected bool, got %T", name, pname, val)
			}
			*param.boolptr = bval
		default:
			panic(param.kind)
		}
	}

	v, err := instance.New()
	if err != nil {
		return err
	}
	p.cached[name] = v
	return assign(name, v, ptr, file, line)
}

func (p *Profile) sorted() []*instance {
	instances := make([]*instance, 0, len(p.instances))
	for _, inst := range p.instances {
		instances = append(instances, inst)
	}
	sort.Slice(instances, func(i, j int) bool {
		return instances[i].name < instances[j].name
	})
	return instances
}

var (
	defaultInit     sync.Once
	defaultInstance *Profile
)

// NewDefault is used to initialize the default profile. It can be
// set by a program before the application profile has been created
// in order to support asynchronous profile retrieval.
var NewDefault = New

// Application returns the default application profile. The default
// instance is initialized during the first call to Application (and thus
// of the package-level methods that operate on the default profile).
// Because of this, Application (and the other package-level methods
// operating on the default profile) should not be called during
// package initialization as doing so means that some global objects
// may not yet have been registered.
func Application() *Profile {
	// TODO(marius): freeze registration after this?
	defaultInit.Do(func() {
		defaultInstance = NewDefault()
	})
	return defaultInstance
}

// Merge merges profile p into the default profile.
// See Profile.Merge for more details.
func Merge(p *Profile) {
	Application().Merge(p)
}

// Parse parses the profile in reader r into the default
// profile. See Profile.Parse for more details.
func Parse(r io.Reader) error {
	return Application().Parse(r)
}

// Instance retrieves the instance with the provided name into the
// provided pointer from the default profile. See Profile.Instance for
// more details.
func Instance(name string, ptr interface{}) error {
	return Application().Instance(name, ptr)
}

// Set sets the value of the parameter named by the provided path on
// the default profile. See Profile.Set for more details.
func Set(path, value string) error {
	return Application().Set(path, value)
}

// Get retrieves the value of the parameter named by the provided path
// on the default profile.
func Get(path string) (value string, ok bool) {
	return Application().Get(path)
}

// Must is a version of get which calls log.Fatal on error.
func Must(name string, ptr interface{}) {
	if err := Instance(name, ptr); err != nil {
		log.Fatal(err)
	}
}

func assign(name string, instance interface{}, ptr reflect.Value, file string, line int) error {
	if ptr == (reflect.Value{}) {
		return nil
	}
	v := reflect.ValueOf(instance)
	if !v.Type().AssignableTo(ptr.Elem().Type()) {
		return fmt.Errorf("%s:%d: %s: instance type %s not assignable to provided type %s",
			file, line, name, v.Type(), ptr.Type())
	}
	ptr.Elem().Set(v)
	return nil
}

func lock(p, q *Profile) (unlock func()) {
	if uintptr(unsafe.Pointer(q)) < uintptr(unsafe.Pointer(p)) {
		p, q = q, p
	}
	p.mu.Lock()
	q.mu.Lock()
	return func() {
		q.mu.Unlock()
		p.mu.Unlock()
	}
}
