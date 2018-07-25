// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package ticket

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"v.io/v23"
	"v.io/v23/security"
	"v.io/v23/vom"
	"v.io/x/lib/vlog"
)

const requiredSuffix = security.ChainSeparator + "_role"

func base64urlVomEncode(i interface{}) (string, error) {
	buf := &bytes.Buffer{}
	closer := base64.NewEncoder(base64.URLEncoding, buf)
	enc := vom.NewEncoder(closer)
	if err := enc.Encode(i); err != nil {
		return "", err
	}
	// Must close the base64 encoder to flush out any partially written
	// blocks.
	if err := closer.Close(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (b *VanadiumBuilder) newVanadiumTicket(ctx *TicketContext) (TicketVanadiumTicket, error) {
	empty := TicketVanadiumTicket{}

	if !strings.HasSuffix(ctx.remoteBlessings.String(), requiredSuffix) {
		return empty, fmt.Errorf("%q doesn't have the required %q suffix", ctx.remoteBlessings.String(), requiredSuffix)
	}

	pubKey := ctx.remoteBlessings.PublicKey()
	expiryCaveat, err := security.NewExpiryCaveat(time.Now().Add(365 * 24 * time.Hour))
	if err != nil {
		return empty, err
	}

	blessing, _ := v23.GetPrincipal(ctx.ctx).BlessingStore().Default()
	resultBlessings, err := v23.GetPrincipal(ctx.ctx).Bless(pubKey, blessing, b.BlessingName, expiryCaveat)
	if err != nil {
		return empty, err
	}

	vlog.VI(1).Infof("resultBlessings: %#v", ctx.remoteBlessings)

	s, err := base64urlVomEncode(resultBlessings)
	if err != nil {
		return empty, err
	}

	return TicketVanadiumTicket{
		Value: VanadiumTicket{
			Blessing: s,
		},
	}, nil
}
