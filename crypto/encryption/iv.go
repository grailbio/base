// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package encryption

import (
	"encoding/hex"
	"fmt"
	"io"
)

// IV represents the initialization vector used to encrypt a block.
type IV []byte

// MarshalJSON marshals an IV as a hex encoded string.
func (iv IV) MarshalJSON() ([]byte, error) {
	if len(iv) == 0 {
		return []byte(`""`), nil
	}
	dst := make([]byte, hex.EncodedLen(len(iv))+2)
	hex.Encode(dst[1:], iv)
	// need to supply leading/trailing double quotes.
	dst[0], dst[len(dst)-1] = '"', '"'
	return dst, nil
}

// UnmarshalJSON unmarshals a hex encoded string into an IV.
func (iv *IV) UnmarshalJSON(data []byte) error {
	// need to strip leading and trailing double quotes
	if data[0] != '"' || data[len(data)-1] != '"' {
		return fmt.Errorf("IV is not quoted")
	}
	data = data[1 : len(data)-1]
	*iv = make([]byte, hex.DecodedLen(len(data)))
	_, err := hex.Decode(*iv, data)
	return err
}

// SetRandSource sets the source of random numbers be used and is intended for
// primarily for testing purposes.
func SetRandSource(rd io.Reader) {
	randomSource = rd
}
