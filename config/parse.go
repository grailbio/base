// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/scanner"
	"unicode"
)

// insertionToks defines the sets of tokens after which
// a semicolon is inserted.
var insertionToks = map[rune]bool{
	scanner.Ident:     true,
	scanner.String:    true,
	scanner.RawString: true,
	scanner.Int:       true,
	scanner.Float:     true,
	scanner.Char:      true,
	')':               true,
	'}':               true,
	']':               true,
}

// def wraps a value to indicate that it is a default.
type def struct{ value interface{} }

// unwrap returns the value v, unwrapped from def.
func unwrap(v interface{}) interface{} {
	if v, ok := v.(def); ok {
		return unwrap(v.value)
	}
	return v
}

// indirect is a type that indicates an indirection.
type indirect string

// GoString renders an indirect type as a string without quotes,
// matching the concrete representation of indirections.
func (i indirect) GoString() string {
	if i == "" {
		return "nil"
	}
	return string(i)
}

// An instance stores a parsed configuration clause.
type instance struct {
	// name is the global name of the instance.
	name string
	// parent is the instance of which this is derived, if any.
	parent string
	// params contains the set of parameters defined by this instance.
	// The values of the parameter map takes on valid config literal
	// values. They are: indirect, bool, int, float64, and string.
	params map[string]interface{}
}

// Merge merges the provided instance into inst. Any
// nondefault parameter values in other are set in this
// instance.
func (inst *instance) Merge(other *instance) {
	if other.parent != "" {
		inst.parent = other.parent
	}
	for k, v := range other.params {
		if _, ok := v.(def); ok {
			continue
		}
		inst.params[k] = v
	}
}

// Equal tells whether two instances are equal.
func (inst *instance) Equal(other *instance) bool {
	if inst.name != other.name || inst.parent != other.parent || len(inst.params) != len(other.params) {
		return false
	}
	for k, v := range inst.params {
		w, ok := other.params[k]
		if !ok {
			return false
		}
		v, w = unwrap(v), unwrap(w)
		switch vval := v.(type) {
		case indirect:
			wval, ok := w.(indirect)
			if !ok || vval != wval {
				return false
			}
		case string:
			wval, ok := w.(string)
			if !ok || vval != wval {
				return false
			}
		case bool:
			wval, ok := w.(bool)
			if !ok || vval != wval {
				return false
			}
		case int:
			wval, ok := w.(int)
			if !ok || vval != wval {
				return false
			}
		case float64:
			wval, ok := w.(float64)
			if !ok || vval != wval {
				return false
			}
		}
	}
	return true
}

// instances stores a collection of named instanes.
type instances map[string]*instance

// Merge merges an instance into this collection.
func (m instances) Merge(inst *instance) {
	if m[inst.name] == nil {
		m[inst.name] = inst
		return
	}
	m[inst.name].Merge(inst)
}

// Equal tells whether instances m is equal to instances n.
func (m instances) Equal(n instances) bool {
	if len(m) != len(n) {
		return false
	}
	for name, minst := range m {
		ninst, ok := n[name]
		if !ok {
			return false
		}
		if !minst.Equal(ninst) {
			return false
		}
	}
	return true
}

// SyntaxString returns a string representation of this instance
// which is also valid config syntax. Docs optionally provides
// documentation for the parameters in the instance.
func (inst *instance) SyntaxString(docs map[string]string) string {
	var b strings.Builder
	if inst.parent == "" {
		b.WriteString("param ")
		b.WriteString(inst.name)
		b.WriteString(" ")
		var prefix string
		if len(inst.params) > 1 {
			b.WriteString("(\n")
			prefix = "\t"
		}
		for k, v := range inst.params {
			b.WriteString(prefix)
			b.WriteString(k)
			b.WriteString(" = ")
			fmt.Fprintf(&b, "%#v", unwrap(v))
			if docs[k] != "" {
				b.WriteString(" // ")
				b.WriteString(docs[k])
			}
			b.WriteString("\n")
		}
		if len(inst.params) > 1 {
			b.WriteString(")\n")
		}
		return b.String()
	}

	b.WriteString("instance ")
	b.WriteString(inst.name)
	b.WriteString(" ")
	b.WriteString(inst.parent)
	if len(inst.params) == 0 {
		b.WriteString("\n")
		return b.String()
	}
	b.WriteString(" (\n")
	for k, v := range inst.params {
		b.WriteString("\t")
		b.WriteString(k)
		b.WriteString(" = ")
		fmt.Fprintf(&b, "%#v", unwrap(v))
		if docs[k] != "" {
			b.WriteString(" // ")
			b.WriteString(docs[k])
		}
		b.WriteString("\n")
	}
	b.WriteString(")\n")
	return b.String()
}

// A parser stores parser state defines the productions
// in the profile grammar.
type parser struct {
	scanner scanner.Scanner
	errors  []string

	insertion bool
	scanned   rune
}

// parse parses the config read by the provided reader into a
// concrete profile into a set of instances. If the reader r
// implements
//
//	Name() string
//
// then this is used as a filename to display positional information
// in error messages.
func parse(r io.Reader) (instances, error) {
	var p parser
	p.scanner.Whitespace &= ^uint64(1 << '\n')
	p.scanner.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanChars |
		scanner.ScanStrings | scanner.ScanRawStrings
	p.scanner.IsIdentRune = func(ch rune, i int) bool {
		return unicode.IsLetter(ch) || (unicode.IsDigit(ch) || ch == '_' || ch == '/' || ch == '-') && i > 0
	}
	if named, ok := r.(interface{ Name() string }); ok {
		filename := named.Name()
		if cwd, err := os.Getwd(); err == nil {
			if rel, err := filepath.Rel(cwd, filename); err == nil && len(rel) < len(filename) {
				filename = rel
			}
		}
		p.scanner.Position.Filename = filename
	}
	p.scanner.Error = func(s *scanner.Scanner, msg string) {
		// TODO(marius): report these in error
		log.Printf("%s: %s", s.Position, msg)
	}
	p.scanner.Init(r)
	if instances, ok := p.toplevel(); ok {
		return instances, nil
	}
	switch len(p.errors) {
	case 0:
		return nil, errors.New("parse error")
	case 1:
		return nil, fmt.Errorf("parse error: %s", p.errors[0])
	default:
		return nil, fmt.Errorf("parse error:\n%s", strings.Join(p.errors, "\n"))
	}
}

// toplevel parses the config grammar. It is as follows:
//
//	toplevel:
//		clause
//		clause ';' toplevel
//		<eof>
//
//	clause:
//		param
//		instance
//
//	param:
//		ident assign
//		ident assignlist
//
//	instance:
//		ident ident
//		ident ident assignlist
//
//	assign:
//		key = value
//
//	assignlist:
//		( list )
//
//	list:
//		assign
//		assign ';' list
//
//	value:
//		'true'
//		'false'
//		'nil'
//		ident
//		integer
//		float
//		string
func (p *parser) toplevel() (insts instances, ok bool) {
	insts = make(instances)
	for {
		switch p.next() {
		case scanner.EOF:
			return
		case ';':
		case scanner.Ident:
			switch p.text() {
			case "param":
				var (
					name   string
					params map[string]interface{}
				)
				name, params, ok = p.param()
				if !ok {
					return
				}
				insts.Merge(&instance{name: name, params: params})
			case "instance":
				var inst *instance
				inst, ok = p.instance()
				if !ok {
					return
				}
				insts.Merge(inst)
			default:
				p.errorf("unrecognized toplevel clause: %s", p.text())
				return nil, false
			}
		}
	}
}

// param:
//	ident assign
//	ident assignlist
func (p *parser) param() (instance string, params map[string]interface{}, ok bool) {
	if p.next() != scanner.Ident {
		p.errorf("expected identifier")
		return
	}
	instance = p.text()
	switch tok := p.peek(); tok {
	case scanner.Ident:
		var (
			key   string
			value interface{}
		)
		key, value, ok = p.assign()
		if !ok {
			return
		}
		params = map[string]interface{}{key: value}
	case '(':
		params, ok = p.assignlist()
	default:
		p.next()
		p.errorf("unexpected: %s", scanner.TokenString(tok))
	}
	return
}

// instance:
//	ident ident
//	ident ident assignlist
func (p *parser) instance() (inst *instance, ok bool) {
	if p.next() != scanner.Ident {
		p.errorf("expected identifier")
		return
	}
	inst = &instance{name: p.text()}
	if p.next() != scanner.Ident {
		p.errorf("expected identifier")
		return
	}
	inst.parent = p.text()
	if p.peek() != '(' {
		ok = true
		return
	}
	inst.params, ok = p.assignlist()
	return
}

// assign:
//	key = value
func (p *parser) assign() (key string, value interface{}, ok bool) {
	if p.next() != scanner.Ident {
		p.errorf("expected identifier")
		return
	}
	key = p.text()
	if p.next() != '=' {
		p.errorf(`expected "="`)
		return
	}
	value, ok = p.value()
	return
}

// assignlist:
//	( list )
//
// list:
//	assign
//	assign ';' list
func (p *parser) assignlist() (assigns map[string]interface{}, ok bool) {
	if p.next() != '(' {
		p.errorf(`parse error: expected "("`)
		return
	}
	assigns = make(map[string]interface{})
	for {
		switch p.peek() {
		default:
			var (
				key   string
				value interface{}
			)
			key, value, ok = p.assign()
			if !ok {
				return
			}
			assigns[key] = value
		case ';':
			p.next()
		case ')':
			p.next()
			ok = true
			return
		}
	}
}

// value:
//	'true'
//	'false'
//	'nil'
//	identifier
//	integer
//	float
//	string
func (p *parser) value() (value interface{}, ok bool) {
	switch p.next() {
	case scanner.Ident:
		switch p.text() {
		case "true":
			return true, true
		case "false":
			return false, true
		case "nil":
			return indirect(""), true
		default:
			return indirect(p.text()), true
		}
	case scanner.Int:
		v, err := strconv.ParseInt(p.text(), 0, 64)
		if err != nil {
			p.errorf("could not parse integer: %v", err)
			return nil, false
		}
		return int(v), true
	case scanner.Float:
		v, err := strconv.ParseFloat(p.text(), 64)
		if err != nil {
			p.errorf("could not parse float: %v", err)
			return nil, false
		}
		return v, true
	case scanner.String, scanner.RawString:
		text, err := strconv.Unquote(p.text())
		if err != nil {
			p.errorf("could not parse string: %v", err)
			return nil, false
		}
		return text, true
	default:
		p.errorf("parse error: not a value")
		return nil, false
	}
}

func (p *parser) next() rune {
	tok := p.peek()
	p.insertion = insertionToks[tok]
	p.scanned = 0
	return tok
}

func (p *parser) peek() rune {
	if p.scanned == 0 {
		p.scanned = p.scanner.Scan()
	}
	if p.insertion && p.scanned == '\n' {
		return ';'
	}
	return p.scanned
}

func (p *parser) text() string {
	return p.scanner.TokenText()
}

func (p *parser) errorf(format string, args ...interface{}) {
	e := fmt.Sprintf("%s: %s", p.scanner.Position, fmt.Sprintf(format, args...))
	p.errors = append(p.errors, e)
}
