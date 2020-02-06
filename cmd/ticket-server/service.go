// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/security/ticket"
	"v.io/v23/context"
	"v.io/v23/rpc"
	"v.io/v23/security/access"
	"v.io/x/lib/vlog"
)

type service struct {
	name       string
	kind       string
	ticket     ticket.Ticket
	perms      access.Permissions
	awsSession *session.Session
}

func (s *service) Get(ctx *context.T, call rpc.ServerCall) (ticket.Ticket, error) {
	// use an empty parameters list
	return (s.GetWithParameters(ctx, call, nil))

}

func (s *service) GetWithParameters(ctx *context.T, call rpc.ServerCall, parameters []ticket.Parameter) (ticket.Ticket, error) {
	vlog.Infof("Get: ctx: %+v call: %+v", ctx, call)
	remoteBlessings := call.Security().RemoteBlessings()
	ticketCtx := ticket.NewTicketContext(ctx, s.awsSession, remoteBlessings)
	switch t := s.ticket.(type) {
	case ticket.TicketAwsTicket:
		return t.Build(ticketCtx, parameters)
	case ticket.TicketS3Ticket:
		return t.Build(ticketCtx, parameters)
	case ticket.TicketSshCertificateTicket:
		return t.Build(ticketCtx, parameters)
	case ticket.TicketEcrTicket:
		return t.Build(ticketCtx, parameters)
	case ticket.TicketTlsServerTicket:
		return t.Build(ticketCtx, parameters)
	case ticket.TicketTlsClientTicket:
		return t.Build(ticketCtx, parameters)
	case ticket.TicketDockerTicket:
		return t.Build(ticketCtx, parameters)
	case ticket.TicketDockerServerTicket:
		return t.Build(ticketCtx, parameters)
	case ticket.TicketDockerClientTicket:
		return t.Build(ticketCtx, parameters)
	case ticket.TicketB2Ticket:
		return t.Build(ticketCtx, parameters)
	case ticket.TicketVanadiumTicket:
		return t.Build(ticketCtx, parameters)
	case ticket.TicketGenericTicket:
		return t.Build(ticketCtx, parameters)
	}
	return nil, fmt.Errorf("not implemented")
}

// GetPermissions implements the Object interface from
// v.io/v23/services/permissions.
func (s *service) GetPermissions(ctx *context.T, call rpc.ServerCall) (perms access.Permissions, version string, _ error) {
	// We don't compose a proper version string because we don't allow setting the
	// permissions.
	return s.perms, "", nil
}

// GetPermissions implements the Object interface from
// v.io/v23/services/permissions.
func (s *service) SetPermissions(ctx *context.T, call rpc.ServerCall, perms access.Permissions, version string) error {
	return fmt.Errorf("not implemented")
}
