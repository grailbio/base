// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"strings"
	"testing"
)

func TestCheckClaims(t *testing.T) {
	googleBlesserInit([]string{"grailbio.com", "contractors.grail.com"})

	cases := []struct {
		claims    claims
		errPrefix string
	}{
		{claims{HostedDomain: "grailbio.com", EmailVerified: true, Email: "user@grailbio.com"}, ""},
		{claims{HostedDomain: "grailbio.com", EmailVerified: true, Email: "user@contractors.grail.com"}, ""},
		{claims{}, "ID token doesn't have a verified email"},
		{claims{EmailVerified: false}, "ID token doesn't have a verified email"},
		{claims{EmailVerified: true, Email: "user@grailbio.com"}, "ID token has a wrong hosted domain:"},
		{claims{HostedDomain: "grailbio.com", EmailVerified: true, Email: "user@gmail.com"}, "ID token does not have a sufix with a authorized email domain"},
		{claims{HostedDomain: "grailbio.com", EmailVerified: true, Email: "user@gmail@.com"}, "ID token does not have a sufix with a authorized email domain"},
	}

	for _, c := range cases {
		err := c.claims.checkClaims()
		if err != nil && (c.errPrefix == "" || !strings.HasPrefix(err.Error(), c.errPrefix)) {
			t.Errorf("checkClaims(%+v): got %q, want prefix %q", c.claims, err, c.errPrefix)
		}
	}
}
