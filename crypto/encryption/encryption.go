// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package encryption

import (
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
)

// KeyID represents the ID used to identify a particular key.
type KeyID []byte

// MarshalJSON marshals a KeyID as a hex encoded string.
func (id KeyID) MarshalJSON() ([]byte, error) {
	if len(id) == 0 {
		return []byte(`""`), nil
	}
	dst := make([]byte, hex.EncodedLen(len(id))+2)
	hex.Encode(dst[1:], id)
	// need to supply leading/trailing double quotes.
	dst[0], dst[len(dst)-1] = '"', '"'
	return dst, nil
}

// UnmarshalJSON unmarshals a hex encoded string into a KeyID.
func (id *KeyID) UnmarshalJSON(data []byte) error {
	// need to strip leading and trailing double quotes
	if data[0] != '"' || data[len(data)-1] != '"' {
		return fmt.Errorf("KeyID is not quoted")
	}
	data = data[1 : len(data)-1]
	*id = make([]byte, hex.DecodedLen(len(data)))
	_, err := hex.Decode(*id, data)
	return err
}

// KeyDescriptor represents a given key and any associated options.
type KeyDescriptor struct {
	Registry string                 `json:"registry"`
	ID       KeyID                  `json:"keyid"`
	Options  map[string]interface{} `json:"options,omitempty"`
}

// Encrypter defines encryption methods.
type Encrypter interface {
	// CipherrextSize returns the size of the ciphertext that
	// that will result from supplied plaintext. It should be used
	// to size the slice supplied to Encrypt.
	CiphertextSize(plaintext []byte) int

	// CiphertextSizeSlices returns the size of the ciphertext that
	// will result from the supplied plaintext slices. It should be used
	// to size the slice supplied to EncryptSlices.
	CiphertextSizeSlices(plaintexts ...[]byte) int

	// Encrypt encrypts the plaintext in src into ciphertext in
	// dst. dst must be at least CiphertextSize() bytes large.
	Encrypt(src, dst []byte) error

	// EncryptSlices encrypts the plaintext slices as a single
	// block. It is intended to avoid the need for an external copy
	// to obtain a single buffer for use with Encrypt. The slices
	// will be decrypted as a single block.
	EncryptSlices(dst []byte, src ...[]byte) error

	// TODO: AEAD method.
	//	Seal(header []byte, p []byte) (int, err)
}

// Decrypter defines decryption methods.
type Decrypter interface {
	// PlaintextSize returns the size of the decrypted plaintext
	// that will result from decrypting the supplied ciphertext.
	// Note that this size will include any checksums enrypted
	// with the original plaintext.
	PlaintextSize(ciphertext []byte) int
	// Decrypt decrypts the ciphertext in src into plaintext stored in dst and
	// returns slices that contain the checksum of the original plaintext
	// and the plaintext. dst should be at least PlainTextSize() bytes big.
	Decrypt(src, dst []byte) (sum, plaintext []byte, err error)
}

type engine struct {
	reg KeyRegistry
	kd  KeyDescriptor
}

var randomSource = rand.Reader

func (e *engine) initIV(b []byte) (iv, buf []byte, err error) {
	bs := e.reg.BlockSize()
	iv, buf = b[:bs], b[bs:]
	n, err := io.ReadFull(randomSource, iv)
	if err != nil {
		err = fmt.Errorf("failed to read %d bytes of random data: %v", len(iv), err)
		return
	}
	if n != len(iv) {
		err = fmt.Errorf("failed to generate complete iv: %d < %d", n, len(b))
	}
	return iv, buf, err
}

func (e *engine) readIV(b []byte) (iv, buf []byte, err error) {
	bs := e.reg.BlockSize()
	if len(b) < bs {
		return nil, nil, fmt.Errorf("failed to read IV")
	}
	return b[:bs], b[bs:], nil
}

// CiphertextSize implements Encrypter.
func (e *engine) CiphertextSize(plaintext []byte) int {
	return e.reg.BlockSize() + e.reg.HMACSize() + len(plaintext)
}

// CiphertextSizeSlices implements Encrypter.
func (e *engine) CiphertextSizeSlices(plaintext ...[]byte) int {
	total := e.reg.BlockSize() + e.reg.HMACSize()
	for _, p := range plaintext {
		total += len(p)
	}
	return total
}

// PlaintextSize impementes Decrypter.
func (e *engine) PlaintextSize(ciphertext []byte) int {
	return e.reg.HMACSize() + len(ciphertext)
}

func (e *engine) setup(dst []byte) ([]byte, hash.Hash, cipher.Stream, error) {
	iv, buf, err := e.initIV(dst)
	if err != nil {
		return nil, nil, nil, err
	}
	// Obtain an hmac hash and cipher.Block from the registry using
	// the specified key ID.
	hmacSum, block, err := e.reg.NewBlock(e.kd.ID)
	if err != nil {
		return nil, nil, nil, err
	}
	stream := cipher.NewCFBEncrypter(block, iv)
	// Generate and encrypt the sum.
	hmacSum.Reset()
	return buf, hmacSum, stream, nil
}

// Encrypt implements Encrypter.
// Encrypt can be used concurrently.
func (e *engine) Encrypt(src, dst []byte) error {
	if len(dst) < e.CiphertextSize(src) {
		return fmt.Errorf("dst is too small, size it using CiphertextSize()")
	}
	buf, hmacSum, stream, err := e.setup(dst)
	if err != nil {
		return err
	}
	hmacSum.Write(src)
	stream.XORKeyStream(buf, hmacSum.Sum(nil))
	// Encrypt plaintext
	stream.XORKeyStream(buf[e.reg.HMACSize():], src)
	return nil
}

// EncryptSlices implements Encrypter.
// EncryptSlices can be used concurrently.
func (e *engine) EncryptSlices(dst []byte, src ...[]byte) error {
	if len(dst) < e.CiphertextSizeSlices(src...) {
		return fmt.Errorf("dst is too small, size it using CiphertextSizeSlices()")
	}
	buf, hmacSum, stream, err := e.setup(dst)
	if err != nil {
		return err
	}
	for _, s := range src {
		hmacSum.Write(s)
	}
	stream.XORKeyStream(buf, hmacSum.Sum(nil))
	buf = buf[e.reg.HMACSize():]
	for _, s := range src {
		stream.XORKeyStream(buf, s)
		buf = buf[len(s):]
	}
	return nil
}

func newEngine(kd KeyDescriptor) (*engine, error) {
	reg, err := Lookup(kd.Registry)
	if err != nil {
		return nil, err
	}
	return &engine{
		reg: reg,
		kd:  kd,
	}, nil
}

// NewEncrypter returns a new encrypter.
// The implementation it returns uses an encrypted HMAC/SHA512 checksum of
// the plaintext to ensure integrity. The format of a block is:
// Initialization Vector (IV)
// encrypted(HMAC(plaintext) + plaintext)
func NewEncrypter(kd KeyDescriptor) (Encrypter, error) {
	return newEngine(kd)
}

// NewDecrypter returns a new decrypter.
func NewDecrypter(kd KeyDescriptor) (Decrypter, error) {
	return newEngine(kd)
}

// Decrypt implements Decrypter.
// Decrypt can be used concurrently.
func (e *engine) Decrypt(src, dst []byte) (sum, plaintext []byte, err error) {
	if len(dst) < e.PlaintextSize(src) {
		return nil, nil, fmt.Errorf("dst is too small, size it using PlaintextSize()")
	}
	// Obtain a cipher.Block from the registry using the specified key ID.
	hmacSum, block, err := e.reg.NewBlock(e.kd.ID)
	if err != nil {
		return nil, nil, err
	}

	iv, buf, err := e.readIV(src)
	if err != nil {
		return nil, nil, err
	}
	stream := cipher.NewCFBDecrypter(block, iv)

	// Decrypt bytes from ciphertext, size buf to the length of the ciphertext
	// including the checksum. Always make sure there is enough room for the
	// checksum, if the buffer is short then we'll get a checksum error.
	sumSize := e.reg.HMACSize()
	if len(buf) >= sumSize {
		dst = dst[:len(buf)]
	}
	stream.XORKeyStream(dst, buf)
	got := dst[:sumSize]
	hmacSum.Reset()
	hmacSum.Write(dst[sumSize:])
	want := hmacSum.Sum(nil)
	if !hmac.Equal(got[:], want) {
		return nil, nil, fmt.Errorf("mismatched checksums: %v != %v ", got, want)
	}
	sum = dst[:sumSize]
	plaintext = dst[sumSize:]
	return
}
