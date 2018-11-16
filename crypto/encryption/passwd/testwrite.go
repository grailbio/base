// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// A simple utility to test writing an encrypted files.
// As such, it is intended to be run interactively and cannot be run as
// part of automated tests. It may run by executing:
//
// $ go run testwrite.go <filename>
//
// It will 'hello\nsafe and secure\nworld\n' to the encrypted file. Use
// $ go run testread.go <filename>
// to decrypt that file.
//
// +build ignore

package main

import (
	"os"

	"github.com/grailbio/base/crypto/encryption"
	_ "github.com/grailbio/base/crypto/encryption/passwd"
	"github.com/grailbio/base/recordio/recordioutil"
)

const msg = `
This is a interactive manual test for writing an encrypted file. Supply a
filename and password to encrypt that file with.

Also, verify that the password is not echoed to the terminal.
`

func main() {
	reg, err := encryption.Lookup("passwd-aes")
	if err != nil {
		panic(err)
	}
	id, err := reg.GenerateKey()
	if err != nil {
		panic(err)
	}
	kd := encryption.KeyDescriptor{Registry: "passwd-aes",
		ID: id,
	}
	opts := recordioutil.WriterOpts{KeyDescriptor: &kd}
	file, err := os.OpenFile(os.Args[1], os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	out, err := recordioutil.NewWriter(file, opts)
	if err != nil {
		panic(err)
	}
	out.Write([]byte("hello\n"))
	out.Write([]byte("safe and secure\n"))
	out.Write([]byte("world\n"))
	out.Flush()
	file.Close()
}
