// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package certificateauthority

import (
	"io/ioutil"
	"testing"

	"bytes"
	"os/exec"
	"time"

	"github.com/grailbio/base/security/keycrypt"
	"github.com/grailbio/testutil/assert"
)

//ssh-keygen -s testdata/ssh_key.pem -I CA -O clear  testdata/ssh_key.pem.pub
func TestAuthority(t *testing.T) {
	sshPEM, err := ioutil.ReadFile("testdata/ssh_key.pem")
	assert.NoError(t, err)
	sshCERT, err := ioutil.ReadFile("testdata/ssh_key.pem-cert.pub")
	assert.NoError(t, err)
	userPubKey, err := ioutil.ReadFile("testdata/user_ssh_key.pem.pub")
	assert.NoError(t, err)

	ca := CertificateAuthority{PrivateKey: keycrypt.Static(sshPEM), Certificate: string(sshCERT)}
	err = ca.Init()
	assert.NoError(t, err)

	cr := CertificateRequest{
		// SSH Public Key that is being signed
		SshPublicKey: []byte(userPubKey),

		// List of host names, or usernames that will be added to the cert
		Principals: []string{"ubuntu"},
		Ttl:        time.Duration(3600) * time.Second,
		KeyID:      "foo",

		CertType: "user",

		CriticalOptions: nil,

		// Extensions to assign to the ssh Certificate
		// The default allow basic function - permit-pty is usually required
		Extensions: []string{
			"permit-X11-forwarding",
			"permit-agent-forwarding",
			"permit-port-forwarding",
			"permit-pty",
			"permit-user-rc",
		},
	}
	execTime := time.Date(2020, time.January, 19, 0, 0, 0, 0, time.UTC)
	sshCert, err := ca.issueWithKeyUsage(execTime, cr)
	assert.NoError(t, err)

	// Check the golang created certificate against the one created with
	// ssh-keygen -s testdata/ssh_key.pem -I foo -V 20200118160000:20200118170000  -n ubuntu testdata/user_ssh_key.pem
	preCreatedUserCert, err := ioutil.ReadFile("testdata/user_ssh_key.pem-cert.pub")
	assert.NoError(t, err)

	cmd := exec.Command("ssh-keygen", "-L", "-f", "-")
	cmd.Stdin = bytes.NewBuffer([]byte(preCreatedUserCert))
	preCreatedOutput, err := cmd.Output()
	assert.NoError(t, err)

	cmd = exec.Command("ssh-keygen", "-L", "-f", "-")
	cmd.Stdin = bytes.NewBuffer([]byte(sshCert))
	output, err := cmd.Output()
	assert.NoError(t, err)

	if string(preCreatedOutput) != string(output) {
		t.Errorf("IssueWithKeyUsage: got %q, want %q", output, preCreatedOutput)
	}
}

func TestValidateCertType(t *testing.T) {
	cases := []struct {
		input     string
		expected  uint32
		expectErr bool
	}{
		{"user", 1, false},
		{"host", 2, false},
		{"FOO", 0, true},
	}
	for _, c := range cases {
		got, err := validateCertType(c.input)
		if got != c.expected {
			t.Errorf("TestValidateCertType(%q): got %q, want %q", c.input, got, c.expected)
		}
		errCheck := (err != nil)
		if errCheck != c.expectErr {
			t.Errorf("TestValidateCertType(%q): got err %q, want %t", c.input, err, c.expectErr)
		}
	}
}
