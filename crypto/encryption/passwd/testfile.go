// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// A simple utility to test writing and reading encrypted files.
// As such, it is intended to be run interactively and cannot be run as
// part of automated tests. It may run by executing:
//
// $ go run testfile.go
//
// +build ignore

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	_ "github.com/grailbio/crypto/encryption/passwd"
	"v.io/x/lib/gosh"
)

const msg = `
This is a interactive manual test for writing/reading encrypted files.
Run it and make sure that:
1. it prints 'hello\nsafe and secure\nworld\n' when you supply matching passwords.
2. it prints an error when the passwords don't match.

In all cases, verify that the password is not echoed to the terminal.
`

func main() {
	sh := gosh.NewShell(nil)
	defer sh.Cleanup()
	tmpdir := sh.MakeTempDir()
	filename := filepath.Join(tmpdir, "test.grail-rpk-kd")

	wr := filepath.Join(tmpdir, "write-grail-rpk-kd")
	rd := filepath.Join(tmpdir, "read-grail-rpk-kd")

	gosh.BuildGoPkg(sh, tmpdir, "./testwrite.go", "-o", wr)
	gosh.BuildGoPkg(sh, tmpdir, "./testread.go", "-o", rd)

	fd, err := syscall.Open("/dev/tty", syscall.O_RDWR, 0)
	if err != nil {
		panic(err)
	}

	for _, name := range []string{wr, rd} {
		tty := os.NewFile(uintptr(fd), "/dev/tty")
		cmd := exec.Command(name, filename)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = tty, tty, tty
		if err := cmd.Start(); err != nil {
			panic(err)
		}
		if err := cmd.Wait(); err != nil {
			panic(err)
		}
	}
}
