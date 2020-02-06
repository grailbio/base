// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package certificateauthority implements an x509 certificate authority.
package certificateauthority

import (
	"crypto/rand"

	"golang.org/x/crypto/ssh"

	"time"

	"github.com/grailbio/base/security/keycrypt"

	"errors"
)

// CertificateAuthority is a ssh certificate authority.
type CertificateAuthority struct {
	// The amount of allowable clock drift between the systems between
	// which certificates are exchanged.
	DriftMargin time.Duration

	// The keycrypt secret that contains the PEM-encoded private key.
	PrivateKey keycrypt.Secret

	// Contains the PEM-encoded Certificate.
	Certificate string

	// The ssh certificate signer. Populated by Init().
	Signer ssh.Signer
}

type CertificateRequest struct {
	// SSH Public Key that is being signed
	SshPublicKey []byte

	// List of host names, or usernames that will be added to the cert
	Principals []string

	// How long this certificate should be valid for
	Ttl time.Duration

	// What identifier should be included in the request
	// This value will be used in logging
	KeyID string

	CertType string // either "user" or "host"

	CriticalOptions []string

	// Extensions to assign to the ssh Certificate
	// The default allow basic function - permit-pty is usually required
	// map[string]string{
	//     "permit-X11-forwarding":   "",
	//     "permit-agent-forwarding": "",
	//     "permit-port-forwarding":  "",
	//     "permit-pty":              "",
	//     "permit-user-rc":          "",
	// }
	Extensions []string
}

func validateCertType(certType string) (uint32, error) {
	switch certType {
	case "user":
		return ssh.UserCert, nil
	case "host":
		return ssh.HostCert, nil
	}
	return 0, errors.New("CertType must be either 'user' or 'host'")
}

// Init initializes the certificate authority. Init extracts the
// authority certificate and private key from ca.Signer.
func (ca *CertificateAuthority) Init() error {
	pkPemBlock, err := ca.PrivateKey.Get()
	if err != nil {
		return err
	}

	// Load the private key
	privateSigner, err := ssh.ParsePrivateKey(pkPemBlock)
	if err != nil {
		return err
	}

	// Load the Certificate
	certificate, _, _, _, err := ssh.ParseAuthorizedKey([]byte(ca.Certificate))
	if err != nil {
		return err
	}
	// Link the private key with its matching Authority Certificate
	ca.Signer, err = ssh.NewCertSigner(certificate.(*ssh.Certificate), privateSigner)
	if err != nil {
		return err
	}

	ca.Signer = privateSigner

	return nil
}

func (ca CertificateAuthority) IssueWithKeyUsage(cr CertificateRequest) (string, error) {
	return ca.issueWithKeyUsage(time.Now(), cr)
}

func (ca CertificateAuthority) issueWithKeyUsage(now time.Time, cr CertificateRequest) (string, error) {

	// Load the Certificate
	pubKey, _, _, _, err := ssh.ParseAuthorizedKey(cr.SshPublicKey)
	if err != nil {
		return "", err
	}

	now = now.Add(-ca.DriftMargin)

	certType, err := validateCertType(cr.CertType)
	if err != nil {
		return "", err
	}

	certificate := &ssh.Certificate{
		Serial:          0,
		Key:             pubKey,
		KeyId:           cr.KeyID,      // Descriptive name of the key (shown in logs)
		ValidPrincipals: cr.Principals, // hostnames (for host cert), or usernames (for client cert)
		ValidAfter:      uint64(now.In(time.UTC).Unix()),
		ValidBefore:     uint64(now.Add(ca.DriftMargin + cr.Ttl).In(time.UTC).Unix()),
		CertType:        certType, // int representing a "user" or "host" type
		Permissions: ssh.Permissions{
			CriticalOptions: convertArrayToMap(cr.CriticalOptions),
			Extensions:      convertArrayToMap(cr.Extensions),
		},
	}

	err = certificate.SignCert(rand.Reader, ca.Signer)
	if err != nil {
		return "", err
	}

	return string(ssh.MarshalAuthorizedKey(certificate)), err
}

// Convert an array of strings into a map of string value pairs
// Value of the key is set to "" which is what the SSH Library wants for extensions and CriticalOptions as flags
func convertArrayToMap(initial []string) map[string]string {
	if initial == nil {
		return nil
	}

	results := map[string]string{}
	for _, key := range initial {
		results[key] = ""
	}
	return results
}
