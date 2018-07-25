// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package certificateauthority

import (
	"crypto/x509"
	"io/ioutil"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/grailbio/base/security/keycrypt"
)

func TestAuthority(t *testing.T) {
	margin := 60 * time.Second
	certPEM, err := ioutil.ReadFile("testdata/cert.pem")
	if err != nil {
		panic(err)
	}
	ca := CertificateAuthority{DriftMargin: margin, Signer: keycrypt.Static(certPEM)}
	if err := ca.Init(); err != nil {
		t.Fatal(err)
	}
	ips := []net.IP{net.IPv4(1, 2, 3, 4)}
	dnses := []string{"test.grail.com"}
	certBytes, priv, err := ca.Issue("test", 10*time.Minute, ips, dnses)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	opts := x509.VerifyOptions{}
	opts.Roots = x509.NewCertPool()
	opts.Roots.AddCert(ca.Cert)
	if _, err := cert.Verify(opts); err != nil {
		t.Fatal(err)
	}
	if err := priv.Validate(); err != nil {
		t.Fatal(err)
	}
	if got, want := priv.Public(), cert.PublicKey; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cert.Subject.CommonName, "test"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := cert.NotBefore, now.Add(-margin); want.Before(got) {
		t.Errorf("wanted %s <= %s", got, want)
	}
	if got, want := cert.NotAfter.Sub(cert.NotBefore), 10*time.Minute+margin; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if cert.IsCA {
		t.Error("cert is CA")
	}
	if got, want := cert.IPAddresses, ips; !ipsEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cert.DNSNames, dnses; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	keyUsage := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	if got, want := cert.ExtKeyUsage, keyUsage; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestAuthorityServerExtKeyUsage(t *testing.T) {
	margin := 60 * time.Second
	certPEM, err := ioutil.ReadFile("testdata/cert.pem")
	if err != nil {
		panic(err)
	}
	ca := CertificateAuthority{DriftMargin: margin, Signer: keycrypt.Static(certPEM)}
	if err := ca.Init(); err != nil {
		t.Fatal(err)
	}
	ips := []net.IP{net.IPv4(1, 2, 3, 4)}
	dnses := []string{"test.grail.com"}
	keyUsage := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	certBytes, priv, err := ca.IssueWithKeyUsage("test", 10*time.Minute, ips, dnses, keyUsage)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		t.Fatal(err)
	}
	opts := x509.VerifyOptions{}
	opts.Roots = x509.NewCertPool()
	opts.Roots.AddCert(ca.Cert)
	if _, err := cert.Verify(opts); err != nil {
		t.Fatal(err)
	}
	if err := priv.Validate(); err != nil {
		t.Fatal(err)
	}
	if got, want := cert.ExtKeyUsage, keyUsage; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestAuthorityClientExtKeyUsage(t *testing.T) {
	margin := 60 * time.Second
	certPEM, err := ioutil.ReadFile("testdata/cert.pem")
	if err != nil {
		panic(err)
	}
	ca := CertificateAuthority{DriftMargin: margin, Signer: keycrypt.Static(certPEM)}
	if err := ca.Init(); err != nil {
		t.Fatal(err)
	}
	ips := []net.IP{net.IPv4(1, 2, 3, 4)}
	dnses := []string{"test.grail.com"}
	keyUsage := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	certBytes, priv, err := ca.IssueWithKeyUsage("test", 10*time.Minute, ips, dnses, keyUsage)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		t.Fatal(err)
	}
	opts := x509.VerifyOptions{}
	opts.Roots = x509.NewCertPool()
	opts.Roots.AddCert(ca.Cert)
	// Client certs can not be verified as a server, expect verification fail.
	if _, err := cert.Verify(opts); err == nil {
		t.Fatal("unexpected valid X509 certificate.")
	}
	if err := priv.Validate(); err != nil {
		t.Fatal(err)
	}
	if got, want := cert.ExtKeyUsage, keyUsage; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func ipsEqual(x, y []net.IP) bool {
	if len(x) != len(y) {
		return false
	}
	for i := range x {
		if !x[i].Equal(y[i]) {
			return false
		}
	}
	return true
}
