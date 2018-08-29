// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build darwin,cgo

// Secrets are stored directly into the macOS Keychain under the name
// com.grail.keycrypt.$namespace; Keycrypt names are stored into the
// account name.
package keychain

import (
	"github.com/grailbio/base/security/keycrypt"
	keychain "github.com/keybase/go-keychain"
)

const prefix = "com.grail.keycrypt."

func init() {
	keycrypt.RegisterFunc("keychain", func(h string) keycrypt.Keycrypt {
		return &Keychain{Namespace: h}
	})
}

var _ keycrypt.Keycrypt = (*Keychain)(nil)

type Keychain struct {
	Namespace string
}

func (k *Keychain) Lookup(name string) keycrypt.Secret {
	return &secret{k, name}
}

type secret struct {
	kc   *Keychain
	name string
}

func (s *secret) Get() ([]byte, error) {
	data, err := keychain.GetGenericPassword(prefix+s.kc.Namespace, s.name, "", "")
	if err == keychain.ErrorItemNotFound {
		return nil, keycrypt.ErrNoSuchSecret
	} else if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *secret) Put(p []byte) error {
	namespace := prefix + s.kc.Namespace

	keychain.DeleteGenericPasswordItem(namespace, s.name)

	item := keychain.NewGenericPassword(namespace, s.name, "", p, "")
	item.SetSynchronizable(keychain.SynchronizableNo)
	item.SetAccessible(keychain.AccessibleWhenUnlocked)

	return keychain.AddItem(item)
}
