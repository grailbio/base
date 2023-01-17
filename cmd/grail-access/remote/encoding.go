// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package remote

import (
	"encoding/base64"
	"fmt"

	"v.io/v23/security"
	"v.io/v23/vom"
)

var b64 = base64.StdEncoding

func encodeBlessings(b security.Blessings) (string, error) {
	bs, err := vom.Encode(b)
	if err != nil {
		return "", fmt.Errorf("vom-encoding blessings: %v", err)
	}
	return b64.EncodeToString(bs), nil
}

func decodeBlessings(s string) (security.Blessings, error) {
	b, err := b64.DecodeString(s)
	if err != nil {
		return security.Blessings{}, fmt.Errorf("base64 decoding blessings string: %v", err)
	}
	var blessings security.Blessings
	if err := vom.Decode(b, &blessings); err != nil {
		return security.Blessings{}, fmt.Errorf("vom-decoding: %v", err)
	}
	return blessings, nil
}

func encodePublicKey(k security.PublicKey) (string, error) {
	der, err := k.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("corrupted public key: %v", err)
	}
	return b64.EncodeToString(der), nil
}

func decodePublicKey(s string) (security.PublicKey, error) {
	bs, err := b64.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("base64-decoding public key: %v", err)
	}
	key, err := security.UnmarshalPublicKey(bs)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling public key: %v", err)
	}
	return key, nil
}
