// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package ticket

import (
	"time"

	"github.com/grailbio/base/security/keycrypt"
	"github.com/grailbio/base/security/ssh/certificateauthority"
	"v.io/x/lib/vlog"
)

const sshDriftMargin = 10 * time.Minute

func (b *SshCertAuthorityBuilder) newSshCertificateTicket(ctx *TicketContext) (TicketSshCertificateTicket, error) {
	sshCert, err := b.genSshCertWithKeyUsage(ctx)

	if err != nil {
		return TicketSshCertificateTicket{}, err
	}

	return TicketSshCertificateTicket{
		Value: SshCertificateTicket{
			Credentials: sshCert,
		},
	}, nil
}

func (b *SshCertAuthorityBuilder) genSshCertWithKeyUsage(ctx *TicketContext) (SshCert, error) {
	vlog.Infof("SshCertAuthorityBuilder: %+v", b)
	empty := SshCert{}

	CaPrivateKey, err := keycrypt.Lookup(b.CaPrivateKey)
	if err != nil {
		return empty, err
	}

	authority := certificateauthority.CertificateAuthority{DriftMargin: sshDriftMargin, PrivateKey: CaPrivateKey, Certificate: b.CaCertificate}
	if err = authority.Init(); err != nil {
		return empty, err
	}

	ttl := time.Duration(b.TtlMin) * time.Minute

	cr := certificateauthority.CertificateRequest{
		// SSH Public Key that is being signed
		SshPublicKey: []byte(b.PublicKey),

		// List of host names, or usernames that will be added to the cert
		Principals: b.Principals,
		Ttl:        ttl,
		KeyID:      ctx.remoteBlessings.String(),

		CertType: "user",

		CriticalOptions: b.CriticalOptions,

		// Extensions to assign to the ssh Certificate
		//  The default allow basic function - permit-pty is usually required
		//  Recommended values are:
		//  []string{
		//   "permit-X11-forwarding",
		//   "permit-agent-forwarding",
		//   "permit-port-forwarding",
		//   "permit-pty",
		//   "permit-user-rc",
		//   }
		Extensions: b.ExtensionsOptions,
	}

	sshCert, err := authority.IssueWithKeyUsage(cr)
	if err != nil {
		return empty, err
	}

	r := SshCert{Cert: sshCert}
	return r, nil
}
