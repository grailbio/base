// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// A simple utility to test reading an encrypted file.
// As such, it is intended to be run interactively and cannot be run as
// part of automated tests. It may run by executing:
//
// $ go run testread.go <filename>
//
// It prints 'hello\nsafe and secure\nworld\n' if you enter the same password
// and file used for testwrite.go.
//
// +build ignore

package main

import (
	"fmt"
	"os"

	_ "github.com/grailbio/base/crypto/encryption/passwd"
	"github.com/grailbio/base/recordio/recordioutil"
)

const msg = `
This is a interactive manual test for reading an encrypted file.
Run it and make sure that:
1. it prints 'hello\nsafe and secure\nworld\n' when you supply matching passwords.

Also, verify that the password is not echoed to the terminal.
`

func main() {
	file, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	opts := recordioutil.ScannerOptsFromName(os.Args[1])
	in, err := recordioutil.NewScanner(file, opts)
	if err != nil {
		panic(err)
	}
	for in.Scan() {
		fmt.Printf("%s", string(in.Bytes()))
	}
	if err := in.Err(); err != nil {
		panic(err)
	}
}
