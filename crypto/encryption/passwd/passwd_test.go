// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package passwd

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha512"
	"strings"
	"testing"

	"github.com/grailbio/base/crypto/encryption"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/expect"
	"golang.org/x/crypto/bcrypt"
)

func TestHash(t *testing.T) {
	pw := []byte("any old pw")

	hash, key, salt, cost, major, minor, err := hashPassword(pw)
	if err != nil {
		t.Fatal(err)
	}

	cmp := func(got, want int) {
		if got != want {
			t.Errorf("%v: got %d, want %d", testutil.Caller(1), got, want)
		}
	}
	cmp(cost, bcrypt.DefaultCost)
	cmp(major, bcrypt.MajorVersion)
	cmp(minor, bcrypt.MinorVersion)
	cmp(len(hash), 32) // sha256
	cmp(len(key), 16)  // aes128

	rkey, err := comparePassword(pw, hash, salt, cost, major, minor)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(key[:], rkey[:]) {
		t.Fatalf("mismatched passwords")
	}

	rkey, err = comparePassword([]byte("oops"), hash, salt, cost, major, minor)
	if err == nil || !strings.Contains(err.Error(), "mismatched") {
		t.Fatalf("failed to detect mismatched passwords")
	}

	if bytes.Equal(key[:], rkey[:]) {
		t.Fatalf("incorrectly matched passwords and yet the keys are the same")
	}
}

func TestRegistry(t *testing.T) {
	reg := NewKeyRegistry()
	encryption.Register("pw", reg)

	if got, want := reg.BlockSize(), 16; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := reg.HMACSize(), 64; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	pw := []byte("any old pw")

	hash, key, salt, cost, major, minor, err := hashPassword(pw)
	if err != nil {
		t.Fatal(err)
	}
	id, err := reg.generateKey(hash, key, salt, cost, major, minor)
	if err != nil {
		t.Fatal(err)
	}

	kd := encryption.KeyDescriptor{Registry: "pw", ID: id}
	enc, err := encryption.NewEncrypter(kd)
	if err != nil {
		t.Fatal(err)
	}
	pt := []byte("my message")
	ct := make([]byte, enc.CiphertextSize(pt))
	enc.Encrypt(pt, ct)

	if bytes.Contains(ct, []byte(pt)) {
		t.Fatalf("encryptiong failed: %s", ct)
	}

	dec, _ := encryption.NewDecrypter(kd)
	buf := make([]byte, dec.PlaintextSize(ct))
	sum, pt1, _ := dec.Decrypt(ct, buf)

	if !bytes.Equal(pt, pt1) {
		t.Fatalf("encryption/decryption failed: %v != %v", pt, pt1)
	}

	h := hmac.New(sha512.New, key[:])
	h.Write(pt)
	if got, want := h.Sum(nil), sum; !bytes.Equal(got[:], want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// Test errors:
	decryptError := func(id string) error {
		kd := encryption.KeyDescriptor{Registry: "pw", ID: []byte(id)}
		dec, _ := encryption.NewDecrypter(kd)
		_, _, err := dec.Decrypt(ct, buf)
		return err
	}
	err = decryptError("{xx")
	expect.HasSubstr(t, err, "failed to unmarshal ID")

	err = decryptError(`{"hash":"\t"}`)
	expect.HasSubstr(t, err, "failed to decode hash")

	err = decryptError(`{"salt":"\t"}`)
	expect.HasSubstr(t, err, "failed to decode salt")

}
