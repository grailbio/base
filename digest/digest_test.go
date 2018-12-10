// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package digest

import (
	"bytes"
	"crypto"
	_ "crypto/md5"
	_ "crypto/sha1"
	_ "crypto/sha256"
	_ "crypto/sha512"
	"encoding/gob"
	"encoding/json"
	"testing"

	_ "golang.org/x/crypto/blake2b"
	_ "golang.org/x/crypto/md4"
	_ "golang.org/x/crypto/ripemd160"
	_ "golang.org/x/crypto/sha3"
)

func TestDigest(t *testing.T) {
	for _, tc := range []struct {
		fn   crypto.Hash
		name string
		out  string
	}{
		{crypto.SHA256, `"sha256"`, "sha256:68e656b251e67e8358bef8483ab0d51c6619f3e7a1a9f0e75838d41ff368f728"},
		{crypto.MD5, `"md5"`, "md5:3adbbad1791fbae3ec908894c4963870"},
		{crypto.SHA512, `"sha512"`, "sha512:6c2618358da07c830b88c5af8c3535080e8e603c88b891028a259ccdb9ac802d0fc0170c99d58affcf00786ce188fc5d753e8c6628af2071c3270d50445c4b1c"},
	} {
		dig := Digester(tc.fn)
		d := dig.FromString("hello, world!")
		if got, want := d.String(), tc.out; got != want {
			t.Fatalf("got %v want %v", got, want)
		}
		dd, err := dig.Parse(tc.out)
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		if got, want := dd, d; got != want {
			t.Fatalf("got %v want %v", got, want)
		}
		dd, err = dig.Parse(tc.out[len(tc.name)-1:])
		if err != nil {
			t.Fatalf("parse failed: %v", err)
		}
		if got, want := dd, d; got != want {
			t.Fatalf("got %v want %v", got, want)
		}
		if got, want := dd.Hash(), tc.fn; got != want {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestReadWrite(t *testing.T) {
	for _, k := range []crypto.Hash{
		crypto.MD4,
		crypto.MD5,
		crypto.SHA1,
		crypto.SHA256,
		crypto.SHA512,
		crypto.SHA3_256,
		crypto.SHA3_512,
		crypto.SHA512_256,
		crypto.BLAKE2b_256,
		crypto.BLAKE2b_384,
		crypto.BLAKE2b_512,
	} {
		dig := Digester(k)
		d := dig.FromString("hello, world!")
		var b bytes.Buffer
		if _, err := WriteDigest(&b, d); err != nil {
			t.Fatal(err)
		}
		dd, err := ReadDigest(&b)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := dd, d; got != want {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	// Test unknown digestHash
	d := Digest{}
	d.h = ^crypto.Hash(0) // make it nonzero but invalid so we don't hit the zero hash panic
	var b bytes.Buffer
	_, err := WriteDigest(&b, d)
	if err == nil {
		t.Fatalf("writing unknown digestHash should have failed")
	}
}

func TestJSON(t *testing.T) {
	for _, tc := range []struct {
		fn   crypto.Hash
		name string
	}{
		{crypto.SHA256, `"sha256"`},
		{crypto.MD5, `"md5"`},
		{crypto.SHA512, `"sha512"`},
	} {
		d := Digester(tc.fn)

		buffer := new(bytes.Buffer)
		encoder := json.NewEncoder(buffer)
		if err := encoder.Encode(d); err != nil {
			t.Fatalf("Encoding failed: %v", err.Error())
		}
		if got, want := buffer.String(), tc.name; got[:len(tc.name)] != want {
			t.Fatalf("got %v want %v", got, want)
		}

		dec := Digester(0)
		decoder := json.NewDecoder(buffer)
		if err := decoder.Decode(&dec); err != nil {
			t.Fatalf("decoded garbage to %v %v", buffer.String(), dec)
		}

		if dec != d {
			t.Fatalf("got %v want %v", dec, d)
		}

		badD := Digester(0)
		encoder = json.NewEncoder(buffer)
		if err := encoder.Encode(badD); err == nil {
			t.Fatalf("Encoding succeeded: %v %v", badD, buffer.String())
		}

		badBuffer := bytes.NewBufferString("garbage")
		decoder = json.NewDecoder(badBuffer)
		if err := decoder.Decode(&dec); err == nil {
			t.Fatalf("decoded garbage to %v %v", buffer.String(), dec)
		}
	}
}

func TestAbbrev(t *testing.T) {
	d := Digester(crypto.SHA256)
	for _, tc := range []struct {
		id     string
		abbrev bool
	}{
		{"9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49", false},
		{"9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c000000000000", false},
		{"9909853c8cada5431400c5f89fe5658e139a0000000000000000000000000000", false},
		{"9909853c8cada5431400c5f89fe0", true},
		{"9909853c8cada5", true},
	} {
		id, err := d.Parse(tc.id)
		if err != nil {
			t.Errorf("parse %v: %v", tc.id, err)
			continue
		}
		if got, want := id.IsAbbrev(), tc.abbrev; got != want {
			t.Errorf("%v: got %v, want %v", id, got, want)
		}
	}
}

func TestTruncate(t *testing.T) {
	d := Digester(crypto.SHA256)
	id, err := d.Parse("9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49")
	if err != nil {
		t.Fatal(err)
	}
	id.Truncate(4)
	if got, want := id.IsAbbrev(), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := id.IsShort(), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := id.String(), "sha256:9909853c00000000000000000000000000000000000000000000000000000000"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := id.HexN(4), "9909853c"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestNPrefix(t *testing.T) {
	d := Digester(crypto.SHA256)
	id, err := d.Parse("9909853c8cada54314ddc5f89fe5658e139aea88cab8c1479a8c35c902b1cb49")
	if err != nil {
		t.Fatal(err)
	}
	for n := 32; n >= 0; n-- {
		id.Truncate(n)
		if got, want := id.NPrefix(), n; got != want {
			t.Errorf("got %v, want %v for %v", got, want, id)
		}
	}
}

func TestGob(t *testing.T) {
	id, err := Parse("sha256:9909853c8cada5431400c5f89fe5658e139aea88cab8c1479a8c35c902b1cb49")
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	if err := enc.Encode(id); err != nil {
		t.Fatal(err)
	}
	dec := gob.NewDecoder(&b)
	var id2 Digest
	if err := dec.Decode(&id2); err != nil {
		t.Fatal(err)
	}
	if got, want := id2, id; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParse(t *testing.T) {
	for _, hash := range []string{"", "<zero>"} {
		h, err := Parse(hash)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := h, (Digest{}); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
