// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package encryption_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/grailbio/base/crypto/encryption"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/encryptiontest"
	"github.com/grailbio/testutil/expect"
)

type randError struct{}

func (r *randError) Read(p []byte) (int, error) {
	return 0, fmt.Errorf("rand failures")
}

type shortRandError struct{}

func (r *shortRandError) Read(p []byte) (int, error) {
	return 10, nil
}

func TestJSON(t *testing.T) {
	out, _ := json.Marshal(&encryption.KeyDescriptor{})
	if got, want := string(out), `{"registry":"","keyid":""}`; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	out, _ = json.Marshal(&encryption.KeyDescriptor{Registry: "x", ID: encryptiontest.TestID})
	if got, want := string(out), `{"registry":"x","keyid":"30313233343536373839616263646566"}`; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	kd := encryption.KeyDescriptor{}
	json.Unmarshal([]byte(`{"keyid":""}`), &kd)
	ekd := encryption.KeyDescriptor{ID: []byte{}}
	if got, want := kd, ekd; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	kd = encryption.KeyDescriptor{}
	json.Unmarshal([]byte(`{"keyid":"ffee"}`), &kd)
	ekd = encryption.KeyDescriptor{ID: []byte{0xff, 0xee}}
	if got, want := kd, ekd; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	err := json.Unmarshal([]byte(`{"keyid": {} }`), &kd)
	if err == nil || !strings.Contains(err.Error(), "not quoted") {
		t.Errorf("missing or wrong error: %v", err)
	}
}

func TestErrors(t *testing.T) {
	reg := encryptiontest.NewFakeAESRegistry()
	if err := encryption.Register("aesTE", reg); err != nil {
		t.Fatal(err)
	}
	// Multiple registrations of the same registry.
	reg = encryptiontest.NewFakeAESRegistry()
	if err := encryption.Register("aesTE", reg); err == nil {
		t.Errorf("expected an error")
	}
	// Missing registry.
	kd := encryption.KeyDescriptor{
		"aesxxx",
		[]byte("any-old-id"),
		nil}
	_, err := encryption.NewEncrypter(kd)
	expect.HasSubstr(t, err, "no such registry")
	_, err = encryption.NewDecrypter(kd)
	expect.HasSubstr(t, err, "no such registry")

	// Buffer too small.
	kd.Registry = "aesTE"
	enc, _ := encryption.NewEncrypter(kd)
	dec, _ := encryption.NewDecrypter(kd)

	err = enc.Encrypt([]byte("something"), []byte{0x00})
	expect.HasSubstr(t, err, "too small")
	err = enc.EncryptSlices([]byte{0x00}, []byte("anything"))
	expect.HasSubstr(t, err, "too small")
	_, _, err = dec.Decrypt([]byte("anything"), []byte{0x00})
	expect.HasSubstr(t, err, "too small")

	// Generate key error.
	reg = encryptiontest.NewFakeAESRegistry()
	reg.Key = encryptiontest.FailGenKey
	_, err = reg.GenerateKey()
	expect.HasSubstr(t, err, "generate-key-failed")

	// NewBlock failure.
	orig := []byte("some-errors")
	src := make([]byte, enc.CiphertextSize(orig))
	enc, _ = encryption.NewEncrypter(encryption.KeyDescriptor{
		Registry: "aesTE", ID: encryptiontest.BadID,
	})
	err = enc.Encrypt(orig, src[:])
	expect.HasSubstr(t, err, "new-block-failed")

	err = enc.EncryptSlices(src[:], orig)
	expect.HasSubstr(t, err, "new-block-failed")

	// Failure to generate IV.
	encryption.SetRandSource(&randError{})
	err = enc.Encrypt(orig, src[:])
	expect.HasSubstr(t, err, "failed to read 16 bytes of random data")

	encryption.SetRandSource(&shortRandError{})
	err = enc.Encrypt(orig, src[:])
	expect.HasSubstr(t, err, "failed to generate complete iv")
	encryption.SetRandSource(rand.Reader)

	enc, _ = encryption.NewEncrypter(encryption.KeyDescriptor{
		Registry: "aesTE", ID: encryptiontest.TestID,
	})
	if err = enc.Encrypt(orig, src[:]); err != nil {
		t.Fatal(err)
	}
	dec, _ = encryption.NewDecrypter(encryption.KeyDescriptor{
		Registry: "aesTE", ID: encryptiontest.BadID,
	})
	dst := make([]byte, dec.PlaintextSize(src))
	_, _, err = dec.Decrypt(src[:], dst[:])
	expect.HasSubstr(t, err, "new-block-failed")

	dec, _ = encryption.NewDecrypter(encryption.KeyDescriptor{
		Registry: "aesTE", ID: encryptiontest.TestID,
	})

	// short IV
	_, _, err = dec.Decrypt(src[:10], dst[:])
	expect.HasSubstr(t, err, "failed to read IV")

	// short Buffer
	_, _, err = dec.Decrypt(src[:20], dst[:])
	expect.HasSubstr(t, err, "mismatched checksums")

	// corrupt the checksum
	src[20] = src[20] + 1
	_, _, err = dec.Decrypt(src[:], dst[:])
	expect.HasSubstr(t, err, "mismatched checksums")
}

var keyDesc encryption.KeyDescriptor
var aesKey []byte

func init() {
	aesReg := encryptiontest.NewFakeAESRegistry()
	if err := encryption.Register("aes", aesReg); err != nil {
		panic(err)
	}
	reg, err := encryption.Lookup("aes")
	if err != nil {
		panic(err)
	}
	id, err := reg.GenerateKey()
	if err != nil {
		panic(err)
	}

	keyDesc = encryption.KeyDescriptor{"aes", id, nil}
	aesKey = aesReg.Key
}

func TestEncryption(t *testing.T) {
	enc, err := encryption.NewEncrypter(keyDesc)
	assert.NoError(t, err)
	dec, err := encryption.NewDecrypter(keyDesc)
	assert.NoError(t, err)

	for _, tc := range []string{
		"",
		"me",
		"oh hello world",
		"oh hello world and something a little longer, really we should test with more data",
	} {
		orig := []byte(tc)
		ctext := make([]byte, enc.CiphertextSize(orig))
		err = enc.Encrypt(orig, ctext)
		assert.NoError(t, err)

		dst := make([]byte, dec.PlaintextSize(ctext))
		sum, ptext, err := dec.Decrypt(ctext, dst)
		assert.NoError(t, err)

		if got, want := ptext, orig; !bytes.Equal(got, want) {
			t.Fatalf("%v: got %v, want %v", orig, got, want)
		}
		hm := hmac.New(sha512.New, aesKey)
		hm.Write(orig)
		if got, want := hm.Sum(nil), sum; !hmac.Equal(got[:], want) {
			t.Fatalf("%v: got %v, want %v", orig, got, want)
		}
	}

	data := [][]byte{
		[]byte(""),
		[]byte("me"),
		[]byte("oh hello world"),
		[]byte("oh hello world and something a little longer, really we should test with more data"),
	}
	orig := bytes.Join(data, nil)
	ctext := make([]byte, enc.CiphertextSizeSlices(data...))
	err = enc.EncryptSlices(ctext, data...)
	assert.NoError(t, err)

	dst := make([]byte, dec.PlaintextSize(ctext))
	sum, ptext, err := dec.Decrypt(ctext, dst)
	assert.NoError(t, err)

	if got, want := ptext, orig; !bytes.Equal(got, want) {
		t.Fatalf("%v: got %v, want %v", orig, got, want)
	}

	hm := hmac.New(sha512.New, aesKey)
	hm.Write(orig)
	if got, want := hm.Sum(nil), sum; !hmac.Equal(got[:], want) {
		t.Fatalf("%v: got %v, want %v", orig, got, want)
	}

}

func TestRandomness(t *testing.T) {
	encryptiontest.RunAtSignificanceLevel(encryptiontest.OnePercent,
		func(s encryptiontest.Significance) bool {
			ptext := make([]byte, 10000)
			enc, err := encryption.NewEncrypter(keyDesc)
			if err != nil {
				return false
			}
			ctext := make([]byte, enc.CiphertextSize(ptext))
			if err := enc.Encrypt(ptext, ctext); err != nil {
				return false
			}
			return encryptiontest.IsRandom(ctext, s)
		})
}
