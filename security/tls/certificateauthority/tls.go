// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package certificateauthority implements an x509 certificate authority.
package certificateauthority

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"time"

	"github.com/grailbio/base/security/keycrypt"
)

// CertificateAuthority is a x509 certificate authority.
type CertificateAuthority struct {
	// The amount of allowable clock drift between the systems between
	// which certificates are exchanged.
	DriftMargin time.Duration
	// The keycrypt secret that contains the PEM-encoded signing
	// certificate and public key.
	Signer keycrypt.Secret
	// The x509 certificate. Populated by Init().
	Cert *x509.Certificate

	key *rsa.PrivateKey
}

// Init initializes the certificate authority. Init extracts the the
// authority certificate and private key from ca.Signer.
func (ca *CertificateAuthority) Init() error {
	pemBlock, err := ca.Signer.Get()
	if err != nil {
		return err
	}
	for {
		var derBlock *pem.Block
		derBlock, pemBlock = pem.Decode(pemBlock)
		if derBlock == nil {
			break
		}
		switch derBlock.Type {
		case "CERTIFICATE":
			ca.Cert, err = x509.ParseCertificate(derBlock.Bytes)
			if err != nil {
				return err
			}
		case "RSA PRIVATE KEY":
			ca.key, err = x509.ParsePKCS1PrivateKey(derBlock.Bytes)
			if err != nil {
				return err
			}
		}
	}
	if ca.Cert == nil || ca.key == nil {
		return errors.New("incomplete certificate")
	}
	return nil
}

// Issue a new certificate with both client and server authentication
// key usage extensions.
func (ca CertificateAuthority) Issue(commonName string, ttl time.Duration, ips []net.IP, dnss []string) ([]byte, *rsa.PrivateKey, error) {
	return ca.IssueWithKeyUsage(commonName, ttl, ips, dnss, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth})
}

// IssueWithKeyUsage a new certificate with the indicated key usage extensions.
func (ca CertificateAuthority) IssueWithKeyUsage(commonName string, ttl time.Duration, ips []net.IP, dnss []string, keyUsage []x509.ExtKeyUsage) ([]byte, *rsa.PrivateKey, error) {
	maxSerial := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, maxSerial)
	if err != nil {
		return nil, nil, err
	}
	now := time.Now().Add(-ca.DriftMargin)
	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:             now,
		NotAfter:              now.Add(ca.DriftMargin + ttl),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           keyUsage,
		BasicConstraintsValid: true,
	}
	template.IPAddresses = append(template.IPAddresses, ips...)
	template.DNSNames = append(template.DNSNames, dnss...)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.CreateCertificate(rand.Reader, &template, ca.Cert, &key.PublicKey, ca.key)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}
