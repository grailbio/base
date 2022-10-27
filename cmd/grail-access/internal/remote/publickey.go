// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package remote

import (
	"fmt"
	"io"

	v23 "v.io/v23"
	"v.io/v23/context"
)

// PrintPublicKey prints the principal of ctx to w (to be read and decoded by
// Bless).
func PrintPublicKey(ctx *context.T, w io.Writer) error {
	p := v23.GetPrincipal(ctx)
	if p == nil {
		// We rely on the caller to set up the principal before making this
		// call.
		return fmt.Errorf("no local principal to bless")
	}
	publicKeyString, err := encodePublicKey(p.PublicKey())
	if err != nil {
		return fmt.Errorf("encoding public key: %v", err)
	}
	if _, err := fmt.Fprintln(w, publicKeyString); err != nil {
		return fmt.Errorf("printing public key: %v", err)
	}
	return nil
}
