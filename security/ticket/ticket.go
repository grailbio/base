// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package ticket

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/security/keycrypt"
	"v.io/v23/context"
	"v.io/v23/security"
	"v.io/x/lib/vlog"
)

// TicketContext wraps the informations that needs to carry around between
// varius ticket functions.
type TicketContext struct {
	ctx             *context.T
	session         *session.Session
	remoteBlessings security.Blessings
}

// NewTicketContext allows creating a TicketContext without unncessary exporting
// its fields.
func NewTicketContext(ctx *context.T, session *session.Session, remoteBlessings security.Blessings) *TicketContext {
	return &TicketContext{
		ctx:             ctx,
		session:         session,
		remoteBlessings: remoteBlessings,
	}
}

// Builder is the interface for building a Ticket.
type Builder interface {
	Build(ctx *TicketContext, parameters []Parameter) (Ticket, error)
}

var (
	_ Builder = (*TicketAwsTicket)(nil)
	_ Builder = (*TicketS3Ticket)(nil)
	_ Builder = (*TicketSshCertificateTicket)(nil)
	_ Builder = (*TicketEcrTicket)(nil)
	_ Builder = (*TicketTlsServerTicket)(nil)
	_ Builder = (*TicketTlsClientTicket)(nil)
	_ Builder = (*TicketDockerTicket)(nil)
	_ Builder = (*TicketDockerServerTicket)(nil)
	_ Builder = (*TicketDockerClientTicket)(nil)
	_ Builder = (*TicketB2Ticket)(nil)
	_ Builder = (*TicketVanadiumTicket)(nil)
	_ Builder = (*TicketGenericTicket)(nil)
)

// Build builds a Ticket by running all the builders.
func (t TicketAwsTicket) Build(ctx *TicketContext, _ []Parameter) (Ticket, error) {
	r := TicketAwsTicket{}
	var err error
	if t.Value.AwsAssumeRoleBuilder != nil {
		r, err = t.Value.AwsAssumeRoleBuilder.newAwsTicket(ctx)
		if err != nil {
			return r, err
		}
		t.Value.AwsAssumeRoleBuilder = nil
	} else if t.Value.AwsSessionBuilder != nil {
		err = t.Value.AwsSessionBuilder.AwsCredentials.kmsInterpolate()
		if err != nil {
			return t, err
		}

		r, err = t.Value.AwsSessionBuilder.newAwsTicket(ctx)
		if err != nil {
			return r, err
		}
		t.Value.AwsSessionBuilder = nil
	}
	r = *mergeOrDie(&r, &t).(*TicketAwsTicket)
	err = r.Value.AwsCredentials.kmsInterpolate()

	return r, err
}

func (t *AwsCredentials) kmsInterpolate() (err error) {
	t.SecretAccessKey, err = kmsInterpolationString(t.SecretAccessKey)
	return err
}

// Build builds a Ticket by running all the builders.
func (t TicketS3Ticket) Build(ctx *TicketContext, _ []Parameter) (Ticket, error) {
	r := TicketS3Ticket{}
	var err error
	if t.Value.AwsAssumeRoleBuilder != nil {
		r, err = t.Value.AwsAssumeRoleBuilder.newS3Ticket(ctx)
		if err != nil {
			return r, err
		}
		t.Value.AwsAssumeRoleBuilder = nil
	} else if t.Value.AwsSessionBuilder != nil {
		err = t.Value.AwsSessionBuilder.AwsCredentials.kmsInterpolate()
		if err != nil {
			return t, err
		}

		r, err = t.Value.AwsSessionBuilder.newS3Ticket(ctx)
		if err != nil {
			return r, err
		}
		t.Value.AwsSessionBuilder = nil
	}
	r = *mergeOrDie(&r, &t).(*TicketS3Ticket)
	err = r.Value.AwsCredentials.kmsInterpolate()
	return r, err
}

// Build builds a Ticket by running all the builders.
func (t TicketSshCertificateTicket) Build(ctx *TicketContext, parameters []Parameter) (Ticket, error) {
	rCompute := TicketSshCertificateTicket{}

	// Populate the ComputeInstances first as input to the SSH CertBuilder
	if t.Value.AwsComputeInstancesBuilder != nil {
		var instanceBuilder = t.Value.AwsComputeInstancesBuilder
		if instanceBuilder.AwsAccountLookupRole != "" {
			instances, err := AwsEc2InstanceLookup(ctx, instanceBuilder)
			if err != nil {
				return nil, err
			}
			rCompute.Value.ComputeInstances = instances
		} else {
			return rCompute, fmt.Errorf("AwsAccountLookupRole required for AwsComputeInstancesBuilder.")
		}
	}

	rSsh := TicketSshCertificateTicket{}
	if t.Value.SshCertAuthorityBuilder != nil {

		// Set the PublicKey parameter on the builder from the input parameters
		// NOTE: If multiple publicKeys are provided as input, use the last one
		for _, param := range parameters {
			if param.Key == "PublicKey" {
				t.Value.SshCertAuthorityBuilder.PublicKey = param.Value
			}
		}

		var err error
		rSsh, err = t.Value.SshCertAuthorityBuilder.newSshCertificateTicket(ctx)
		if err != nil {
			return rSsh, err
		}
		t.Value.SshCertAuthorityBuilder = nil
	}

	r := *mergeOrDie(&rCompute, &rSsh).(*TicketSshCertificateTicket)
	return *mergeOrDie(&r, &t).(*TicketSshCertificateTicket), nil
}

// Build builds a Ticket by running all the builders.
func (t TicketEcrTicket) Build(ctx *TicketContext, _ []Parameter) (Ticket, error) {
	r := TicketEcrTicket{}
	if t.Value.AwsAssumeRoleBuilder != nil {
		var err error
		r, err = t.Value.AwsAssumeRoleBuilder.newEcrTicket(ctx)
		if err != nil {
			return r, err
		}
		t.Value.AwsAssumeRoleBuilder = nil
	}
	return *mergeOrDie(&r, &t).(*TicketEcrTicket), nil
}

// Build builds a Ticket by running all the builders.
func (t TicketTlsServerTicket) Build(ctx *TicketContext, _ []Parameter) (Ticket, error) {
	r := TicketTlsServerTicket{}
	if t.Value.TlsCertAuthorityBuilder != nil {
		var err error
		r, err = t.Value.TlsCertAuthorityBuilder.newTlsServerTicket(ctx)
		if err != nil {
			return r, err
		}
		t.Value.TlsCertAuthorityBuilder = nil
	}
	return *mergeOrDie(&r, &t).(*TicketTlsServerTicket), nil
}

// Build builds a Ticket by running all the builders.
func (t TicketTlsClientTicket) Build(ctx *TicketContext, _ []Parameter) (Ticket, error) {
	r := TicketTlsClientTicket{}
	if t.Value.TlsCertAuthorityBuilder != nil {
		var err error
		r, err = t.Value.TlsCertAuthorityBuilder.newTlsClientTicket(ctx)
		if err != nil {
			return r, err
		}
		t.Value.TlsCertAuthorityBuilder = nil
	}
	return *mergeOrDie(&r, &t).(*TicketTlsClientTicket), nil
}

// Build builds a Ticket by running all the builders.
func (t TicketDockerTicket) Build(ctx *TicketContext, _ []Parameter) (Ticket, error) {
	r := TicketDockerTicket{}
	if t.Value.TlsCertAuthorityBuilder != nil {
		var err error
		r, err = t.Value.TlsCertAuthorityBuilder.newDockerTicket(ctx)
		if err != nil {
			return r, err
		}
		t.Value.TlsCertAuthorityBuilder = nil
	}
	return *mergeOrDie(&r, &t).(*TicketDockerTicket), nil
}

// Build builds a Ticket by running all the builders.
func (t TicketDockerServerTicket) Build(ctx *TicketContext, _ []Parameter) (Ticket, error) {
	r := TicketDockerServerTicket{}
	if t.Value.TlsCertAuthorityBuilder != nil {
		var err error
		r, err = t.Value.TlsCertAuthorityBuilder.newDockerServerTicket(ctx)
		if err != nil {
			return r, err
		}
		t.Value.TlsCertAuthorityBuilder = nil
	}
	return *mergeOrDie(&r, &t).(*TicketDockerServerTicket), nil
}

// Build builds a Ticket by running all the builders.
func (t TicketDockerClientTicket) Build(ctx *TicketContext, _ []Parameter) (Ticket, error) {
	r := TicketDockerClientTicket{}
	if t.Value.TlsCertAuthorityBuilder != nil {
		var err error
		r, err = t.Value.TlsCertAuthorityBuilder.newDockerClientTicket(ctx)
		if err != nil {
			return r, err
		}
		t.Value.TlsCertAuthorityBuilder = nil
	}
	return *mergeOrDie(&r, &t).(*TicketDockerClientTicket), nil
}

// Build builds a Ticket by running all the builders.
func (t TicketB2Ticket) Build(_ *TicketContext, _ []Parameter) (Ticket, error) {
	r := TicketB2Ticket{}
	if t.Value.B2AccountAuthorizationBuilder != nil {
		var err error
		r, err = t.Value.B2AccountAuthorizationBuilder.newB2Ticket()
		if err != nil {
			return r, err
		}
		t.Value.B2AccountAuthorizationBuilder = nil
	}
	return *mergeOrDie(&r, &t).(*TicketB2Ticket), nil
}

// Build builds a Ticket by running all the builders.
func (t TicketVanadiumTicket) Build(ctx *TicketContext, _ []Parameter) (Ticket, error) {
	r := TicketVanadiumTicket{}
	if t.Value.VanadiumBuilder != nil {
		var err error
		r, err = t.Value.VanadiumBuilder.newVanadiumTicket(ctx)
		if err != nil {
			return r, err
		}
		t.Value.VanadiumBuilder = nil
	}
	return *mergeOrDie(&r, &t).(*TicketVanadiumTicket), nil
}

// Build builds a Ticket.
func (t TicketGenericTicket) Build(_ *TicketContext, _ []Parameter) (Ticket, error) {
	r := TicketGenericTicket{}
	r = *mergeOrDie(&r, &t).(*TicketGenericTicket)
	var err error
	r.Value.Data, err = kmsInterpolationBytes(r.Value.Data)
	return r, err
}

// merge i2 in i1 by overwriting in i1 all the non-zero fields in i2. The i1
// and i2 needs to be references to the same type. Only simple types (bool,
// numeric, string) and string are supported.
func mergeOrDie(i1, i2 interface{}) interface{} {
	if reflect.DeepEqual(i1, i2) {
		return i1
	}
	v1, v2 := reflect.ValueOf(i1).Elem(), reflect.ValueOf(i2).Elem()
	k1, k2 := v1.Kind(), v2.Kind()
	if k1 != k2 {
		vlog.Fatalf("different types in merge: %+v (%s) vs %v (%s)", v1, v1.Kind(), v2, v2.Kind())
	}
	switch k1 {
	case reflect.Struct:
		for i := 0; i < v1.NumField(); i++ {
			f1, f2 := v1.Field(i), v2.Field(i)
			if !f1.CanSet() {
				continue
			}
			v := mergeOrDie(f1.Addr().Interface(), f2.Addr().Interface())
			f1.Set(reflect.Indirect(reflect.ValueOf(v)))
		}
	case reflect.Map:
		// TODO(razvanm): figure out why the default doesn't work.
		if v2.Len() > 0 {
			v1.Set(v2)
		}
	default:
		zero := reflect.Zero(v2.Type()).Interface()
		if !reflect.DeepEqual(v2.Interface(), zero) {
			v1.Set(v2)
		}
	}
	return i1
}

// kmsInterpolation takes a string and, if the values is a 'kms://' URL it
// returns the corresponding keycrypt values.
func kmsInterpolationString(s string) (string, error) {
	if !strings.HasPrefix(s, "kms://") {
		return s, nil
	}

	secret, err := keycrypt.Lookup(s)
	if err != nil {
		return "", fmt.Errorf("keycrypt.Lookup(%q): %v", s, err)
	}

	secretBytes, err := secret.Get()
	if err != nil {
		return "", fmt.Errorf("Secret.Get(%q): %v", s, err)
	}

	return string(secretBytes), nil
}

func kmsInterpolationBytes(b []byte) ([]byte, error) {
	if !bytes.HasPrefix(b, []byte("kms://")) {
		return b, nil
	}

	secret, err := keycrypt.Lookup(string(b))
	if err != nil {
		return nil, fmt.Errorf("keycrypt.Lookup(%q): %v", b, err)
	}

	secretBytes, err := secret.Get()
	if err != nil {
		return nil, fmt.Errorf("Secret.Get(%q): %v", b, err)
	}

	return secretBytes, nil
}
