// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package awssession

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/security/ticket"
	"v.io/v23/context"
)

// Provider implements the aws/credentials.Provider interface using a GRAIL
// ticket.
type Provider struct {
	// Ctx contains the Vanadium context used to make the call to ticket-server
	// in response to calls to Retrieve(). Canceling the context will cause the
	// calls to Retrieve() to fail and IsExpire() to always return true.
	Ctx *context.T

	// Timeout indicates what timeout to set for the Vanadium calls.
	Timeout time.Duration

	// Ticket contains the last GRAIL ticket retrieved by a call to Retrieve().
	Ticket ticket.Ticket

	// Expiration indicates when the AWS credentials will expire.
	Expiration time.Time

	// TicketPath indicates what Vanadium object name to use to retrieve the
	// ticket.
	TicketPath string

	// ExpiryWindow allows triggering a refresh before the AWS credentials
	// actually expire.
	ExpiryWindow time.Duration
}

var _ credentials.Provider = (*Provider)(nil)

// Retrieve implements the github.com/aws/aws-sdk-go/aws/credentials.Provider
// interface.
func (p *Provider) Retrieve() (credentials.Value, error) {
	ctx := p.Ctx
	if p.Timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.Timeout)
		defer cancel()
	}
	var err error
	p.Ticket, err = ticket.TicketServiceClient(p.TicketPath).Get(ctx)
	if err != nil {
		return credentials.Value{}, err
	}
	return p.retrieve()
}

// retrieve implements some logic that would be harder to test if it's part of
// the Retrieve() function.
func (p *Provider) retrieve() (credentials.Value, error) {
	awsTicket, ok := p.Ticket.(ticket.TicketAwsTicket)
	if !ok {
		return credentials.Value{}, fmt.Errorf("bad ticket type %T for %q, want %T", p.Ticket, p.TicketPath, awsTicket)
	}

	if awsTicket.Value.AwsCredentials.Expiration != "" {
		var err error
		p.Expiration, err = time.Parse(time.RFC3339, awsTicket.Value.AwsCredentials.Expiration)
		if err != nil {
			p.Ticket = nil
			return credentials.Value{}, errors.E(err, fmt.Sprintf("%q: error parsing %q", p.TicketPath, awsTicket.Value.AwsCredentials.Expiration))
		}
	}
	return credentials.Value{
		AccessKeyID:     awsTicket.Value.AwsCredentials.AccessKeyId,
		SecretAccessKey: awsTicket.Value.AwsCredentials.SecretAccessKey,
		SessionToken:    awsTicket.Value.AwsCredentials.SessionToken,
		ProviderName:    "ticket",
	}, nil
}

// IsExpired implements the github.com/aws/aws-sdk-go/aws/credentials.Provider
// interface.
func (p *Provider) IsExpired() bool {
	var r bool
	if p.Ticket == nil {
		r = true
	} else if !p.Expiration.IsZero() {
		r = time.Now().Add(p.ExpiryWindow).After(p.Expiration)
	}
	return r
}
