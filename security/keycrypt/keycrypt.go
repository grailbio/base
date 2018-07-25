// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package keycrypt implements an API for storing and retrieving
// opaque blobs of data stored in a secure fashion. Keycrypt multiplexes
// several backends, both local (e.g., macOS Keychain) and remote (e.g.,
// AWS's KMS and S3).
package keycrypt

import (
	"encoding/json"
	"errors"
)

var ErrNoSuchSecret = errors.New("no such secret")

// Secret represents a single object. Secret objects are
// uninterpreted bytes that are stored securely.
type Secret interface {
	// Retrieve the current value of this secret. If the secret does not
	// exist, Get returns ErrNoSuchSecret.
	Get() ([]byte, error)
	// Write a new value for this secret.
	Put([]byte) error
}

// Interface Keycrypt represents a secure secret storage.
type Keycrypt interface {
	// Look up the named secret. A secret is returned even if it does
	// not yet exist. In this case, Secret.Get will return
	// ErrNoSuchSecret.
	Lookup(name string) Secret
}

type Resolver interface {
	Resolve(host string) Keycrypt
}

type funcResolver func(string) Keycrypt

func (f funcResolver) Resolve(host string) Keycrypt { return f(host) }
func ResolverFunc(f func(string) Keycrypt) Resolver { return funcResolver(f) }

// Retrieve the content from a secret and unmarshal
// it into a value.
func GetJSON(s Secret, v interface{}) error {
	b, err := s.Get()
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// Marshal a value and write it into a secret.
func PutJSON(s Secret, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.Put(b)
}

// Implements Secret.
type static []byte

func Static(b []byte) Secret          { return static(b) }
func (s static) Get() ([]byte, error) { return []byte(s), nil }
func (s static) Put([]byte) error     { return errors.New("illegal operation") }

type nonexistent int

func Nonexistent() Secret                { return nonexistent(0) }
func (nonexistent) Get() ([]byte, error) { return nil, ErrNoSuchSecret }
func (nonexistent) Put([]byte) error     { return errors.New("illegal operation") }
