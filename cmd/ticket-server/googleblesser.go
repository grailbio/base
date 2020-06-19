// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	oidc "github.com/coreos/go-oidc"
	v23context "v.io/v23/context"
	"v.io/v23/rpc"
	"v.io/v23/security"
	"v.io/x/lib/vlog"
)

const (
	issuer   = "https://accounts.google.com"
	audience = "27162366543-edih9cqc3t8p5hn9ord1k1n7h4oajfhm.apps.googleusercontent.com"

	extensionPrefix = "google"
)

var (
	hostedDomains []string
)

func googleBlesserInit(googleUserDomainList []string) {
	hostedDomains = googleUserDomainList
}

func (c *claims) checkClaims() error {
	if !c.EmailVerified {
		return fmt.Errorf("ID token doesn't have a verified email")
	}

	if !stringInSlice(hostedDomains, c.HostedDomain) {
		return fmt.Errorf("ID token has a wrong hosted domain: got %q, want %q", c.HostedDomain, strings.Join(hostedDomains, ","))
	}

	if !stringInSlice(hostedDomains, emailDomain(c.Email)) {
		return fmt.Errorf("ID token does not have a sufix with a authorized email domain (%q): %q", strings.Join(hostedDomains, ","), c.Email)
	}
	return nil
}

type claims struct {
	HostedDomain  string `json:"hd"`
	EmailVerified bool   `json:"email_verified"`
	Email         string `json:"email"`
}

type googleBlesser struct {
	verifier           *oidc.IDTokenVerifier
	expirationInterval time.Duration
}

func newGoogleBlesser(expiration time.Duration, domains []string) *googleBlesser {
	googleBlesserInit(domains)

	provider, err := oidc.NewProvider(context.Background(), issuer)
	if err != nil {
		vlog.Fatal(err)
	}
	return &googleBlesser{
		verifier:           provider.Verifier(&oidc.Config{ClientID: audience}),
		expirationInterval: expiration,
	}
}

func (blesser *googleBlesser) BlessGoogle(ctx *v23context.T, call rpc.ServerCall, idToken string) (security.Blessings, error) {
	remoteAddress := call.RemoteEndpoint().Address
	vlog.Infof("idtoken(%s): %d bytes", remoteAddress, len(idToken))
	vlog.VI(1).Infof("idtoken(%s): %v", remoteAddress, idToken)
	var empty security.Blessings

	oidcIDToken, err := blesser.verifier.Verify(ctx, idToken)
	if err != nil {
		return empty, err
	}
	vlog.VI(1).Infof("oidcIDToken: %+v", oidcIDToken)
	var claims claims
	if err := oidcIDToken.Claims(&claims); err != nil {
		return empty, nil
	}
	vlog.VI(1).Infof("claims: %+v", claims)

	if err := claims.checkClaims(); err != nil {
		return empty, err
	}

	// ext will be something like 'google:razvanm@grailbio.com'.
	ext := strings.Join([]string{extensionPrefix, claims.Email}, security.ChainSeparator)

	securityCall := call.Security()
	if securityCall.LocalPrincipal() == nil {
		return empty, fmt.Errorf("server misconfiguration")
	}

	pubKey := securityCall.RemoteBlessings().PublicKey()
	caveat, err := security.NewExpiryCaveat(time.Now().Add(blesser.expirationInterval))
	if err != nil {
		return empty, err
	}
	return securityCall.LocalPrincipal().Bless(pubKey, securityCall.LocalBlessings(), ext, caveat)
}
