// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package file implements a file-based keycrypt.
package file

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/grailbio/base/security/keycrypt"
)

func init() {
	keycrypt.RegisterFunc("file", func(h string) keycrypt.Keycrypt {
		return &crypt{"/"}
	})
	keycrypt.RegisterFunc("localfile", func(h string) keycrypt.Keycrypt {
		// h is taken to be a namespace
		return &crypt{filepath.Join(os.Getenv("HOME"), ".keycrypt", h)}
	})
}

type crypt struct{ path string }

func (c *crypt) Lookup(name string) keycrypt.Secret {
	return fileSecret(filepath.Join(c.path, name))
}

type fileSecret string

func (f fileSecret) Get() ([]byte, error) {
	if _, err := os.Stat(string(f)); os.IsNotExist(err) {
		return nil, nil
	}
	return ioutil.ReadFile(string(f))
}

func (f fileSecret) Put(b []byte) error {
	dir := filepath.Dir(string(f))
	os.MkdirAll(dir, 0777) // best effort
	tmpfile, err := ioutil.TempFile(dir, "")
	if err != nil {
		return err
	}
	// Best effort because it's not vital and it doesn't work on Windows.
	tmpfile.Chmod(0644)
	if _, err := tmpfile.Write(b); err != nil {
		return err
	}
	if err := tmpfile.Close(); err != nil {
		return err
	}
	return os.Rename(tmpfile.Name(), string(f))
}
