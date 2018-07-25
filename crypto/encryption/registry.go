// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package encryption

import (
	"crypto/cipher"
	"fmt"
	"hash"
	"sync"
)

type db struct {
	sync.Mutex
	registries map[string]KeyRegistry
}

var registries = &db{registries: map[string]KeyRegistry{}}

// KeyRegistry represents a database of keys for a particular cipher, ie.
// implementations of KeyRegistry manage the keys for a particular cipher.
// AEAD is supported by wrapping the block ciphers provided.
type KeyRegistry interface {
	GenerateKey() (ID []byte, err error)
	BlockSize() int
	HMACSize() int
	NewBlock(ID []byte, opts ...interface{}) (hmac hash.Hash, block cipher.Block, err error)
	NewGCM(block cipher.Block, opts ...interface{}) (aead cipher.AEAD, err error)
}

// Lookup returns the key registry, if any, named by the supplied name.
func Lookup(name string) (KeyRegistry, error) {
	registries.Lock()
	defer registries.Unlock()
	r := registries.registries[name]
	if r == nil {
		return nil, fmt.Errorf("no such registry: %v", name)
	}
	return r, nil
}

// Register registers a new KeyRegistry using the supplied name.
func Register(name string, registry KeyRegistry) error {
	registries.Lock()
	defer registries.Unlock()
	if _, present := registries.registries[name]; present {
		return fmt.Errorf("already registered: %v", registry)
	}
	registries.registries[name] = registry
	return nil
}
