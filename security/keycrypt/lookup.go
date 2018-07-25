// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package keycrypt

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
)

var (
	mu           sync.Mutex
	resolvers    = map[string]Resolver{}
	localSchemes = []string{"keychain", "localfile"}
)

// Register associates a Resolver with a scheme.
func Register(scheme string, resolver Resolver) {
	mu.Lock()
	resolvers[scheme] = resolver
	mu.Unlock()
}

// For testing.
func unregister(scheme string) {
	mu.Lock()
	resolvers[scheme] = nil
	mu.Unlock()
}

// RegisterFunc associates a Resolver (given by a func)
// with a scheme.
func RegisterFunc(scheme string, f func(string) Keycrypt) {
	Register(scheme, ResolverFunc(f))
}

// Lookup retrieves a secret based on a URL, in the standard form:
// scheme://host/path. The URL is interpreted according to the
// Resolver registered with the given scheme. The scheme "local"
// is a special scheme that attempts known local storage schemes:
// first "keychain", and then "file".
func Lookup(rawurl string) (Secret, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	mu.Lock()
	defer mu.Unlock()
	var r Resolver
	if u.Scheme == "local" {
		for _, s := range localSchemes {
			r = resolvers[s]
			if r != nil {
				break
			}
		}
		if r == nil {
			return nil, fmt.Errorf("no local resolvers found, tried: %s", strings.Join(localSchemes, ", "))
		}
	} else {
		r = resolvers[u.Scheme]
	}
	if r == nil {
		return nil, fmt.Errorf("unknown scheme \"%s\"", u.Scheme)
	}
	name := u.Path
	if name != "" && name[0] == '/' {
		name = name[1:]
	}
	return r.Resolve(u.Host).Lookup(name), nil
}

// Get data from a keycrypt URL.
func Get(rawurl string) ([]byte, error) {
	s, err := Lookup(rawurl)
	if err != nil {
		return nil, err
	}
	return s.Get()
}

// Put writes data to a keycrypt URL.
func Put(rawurl string, data []byte) error {
	s, err := Lookup(rawurl)
	if err != nil {
		return err
	}
	return s.Put(data)
}
