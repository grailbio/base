// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package remote

import (
	"fmt"
	"io"

	v23 "v.io/v23"
	"v.io/v23/context"
	"v.io/v23/security"
)

// ReceiveBlessings reads encoded blessings from r and sets them as the default
// blessings and as blessings for all principal peers.
func ReceiveBlessings(ctx *context.T, r io.Reader) error {
	p := v23.GetPrincipal(ctx)
	if p == nil {
		// We rely on the caller to set up the principal before making this
		// call.
		return fmt.Errorf("no local principal to bless")
	}
	// Read a single-line encoding of the received blessing, and set them as
	// both the default and for all peer principals.
	input, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("reading input: %v", err)
	}
	b, err := decodeBlessings(string(input))
	if err != nil {
		return fmt.Errorf("decoding blessings string: %v", err)
	}
	store := p.BlessingStore()
	if err := store.SetDefault(b); err != nil {
		return fmt.Errorf("setting blessings %v as default: %v", b, err)
	}
	if _, err := store.Set(b, security.AllPrincipals); err != nil {
		return fmt.Errorf("setting blessings %v for peers %v: %v", b, security.AllPrincipals, err)
	}
	if err := security.AddToRoots(p, b); err != nil {
		return fmt.Errorf("adding blessings to recognized roots: %v", err)
	}
	return nil
}
