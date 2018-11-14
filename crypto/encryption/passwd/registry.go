// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package passwd

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"sync"

	"github.com/grailbio/base/crypto/encryption"
)

// AESKey represents a 16 byte AES key for AES 128.
type AESKey [16]byte

// AES represents a passwd based key registry that uses AES encryption.
type AES struct {
	mu   sync.Mutex
	keys map[string]AESKey
}

// NewKeyRegistry creates a new key registry.
func NewKeyRegistry() *AES {
	return &AES{
		keys: map[string]AESKey{},
	}
}

func init() {
	if err := encryption.Register("passwd-aes", NewKeyRegistry()); err != nil {
		panic(err)
	}
}

// SetIDAndKey stores the specified ID and Key in the key registry.
func (c *AES) SetIDAndKey(ID []byte, key AESKey) {
	c.mu.Lock()
	c.keys[string(ID)] = key
	c.mu.Unlock()
}

// The ID field in the key registry is used to encode the metadata
// and the hash of the password.
type idPayload struct {
	Hash  string `json:"hash"`
	Salt  string `json:"salt"`
	Cost  int    `json:"cost"`
	Major int    `json:"major"`
	Minor int    `json:"minor"`
}

// GenerateKey implements encryption.KeyRegistry.
func (c *AES) GenerateKey() (ID []byte, err error) {
	hash, key, salt, cost, major, minor, err := ReadAndHashPassword()
	if err != nil {
		return nil, err
	}
	return c.generateKey(hash, key, salt, cost, major, minor)
}

func (c *AES) generateKey(hash []byte, key AESKey, salt []byte, cost, major, minor int) (ID []byte, err error) {
	payload := idPayload{
		hex.EncodeToString(hash),
		hex.EncodeToString(salt),
		cost,
		major,
		minor,
	}
	id, err := json.Marshal(&payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %v", err)
	}
	c.SetIDAndKey(id, key)
	return id, nil
}

// BlockSize implements encryption.KeyRegistry.
func (c *AES) BlockSize() int {
	return aes.BlockSize
}

// HMACSize implements encryption.KeyRegistry.
func (c *AES) HMACSize() int {
	return sha512.Size
}

var zeroKey = AESKey{}

// NewBlock implements encryption.KeyRegistry.
func (c *AES) NewBlock(ID []byte, opts ...interface{}) (hmc hash.Hash, block cipher.Block, err error) {
	id := idPayload{}
	if err := json.Unmarshal(ID, &id); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal ID: %v", err)
	}
	c.mu.Lock()
	key := c.keys[string(ID)]
	c.mu.Unlock()
	hash, err := hex.DecodeString(id.Hash)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode hash: %v", err)
	}
	salt, err := hex.DecodeString(id.Salt)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode salt: %v", err)
	}
	if bytes.Equal(key[:], zeroKey[:]) {
		key, err = ReadAndComparePassword(hash, salt, id.Cost, id.Major, id.Minor)
		if err != nil {
			return nil, nil, err
		}
	}
	c.mu.Lock()
	c.keys[string(ID)] = key
	c.mu.Unlock()
	hmc = hmac.New(sha512.New, key[:])
	blk, err := aes.NewCipher(key[:])
	return hmc, blk, err
}

// NewGCM implements encryption.KeyRegistry.
func (c *AES) NewGCM(block cipher.Block, opts ...interface{}) (aead cipher.AEAD, err error) {
	return nil, fmt.Errorf("not implemented yet")
}
