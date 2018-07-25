// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/common/log"
	"github.com/grailbio/base/security/ticket"
	"v.io/v23/context"
	"v.io/v23/rpc"
	"v.io/v23/security/access"
)

type service struct {
	name       string
	kind       string
	ticket     ticket.Ticket
	perms      access.Permissions
	awsSession *session.Session
	controls   map[ticket.Control]bool
}

func (s *service) log(ctx *context.T, call rpc.ServerCall, parameters []ticket.Parameter, args map[string]string) {
	logArgs := make([]interface{}, len(parameters)+len(args)*2+2)
	i := 0
	for _, p := range parameters {
		logArgs[i] = p.Key
		logArgs[i+1] = p.Value
		i += 2
	}
	for k, v := range args {
		logArgs[i] = k
		logArgs[i+1] = v
		i += 2
	}
	log.Info(ctx, "Fetching ticket.", logArgs...)
}

func (s *service) get(ctx *context.T, call rpc.ServerCall, parameters []ticket.Parameter, args map[string]string) (ticket.Ticket, error) {
	s.log(ctx, call, parameters, args)
	if ok, err := s.checkControls(ctx, call, args); !ok {
		return nil, err
	}
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

func (s *service) Get(ctx *context.T, call rpc.ServerCall) (ticket.Ticket, error) {
	log.Info(ctx, "get request", "blessing", call.Security().RemoteBlessings(), "ticket", call.Suffix())
	return s.get(ctx, call, nil, nil)
}

func (s *service) GetWithArgs(ctx *context.T, call rpc.ServerCall, args map[string]string) (ticket.Ticket, error) {
	return s.get(ctx, call, nil, args)
}

func (s *service) GetWithParameters(ctx *context.T, call rpc.ServerCall, parameters []ticket.Parameter) (ticket.Ticket, error) {
	return s.get(ctx, call, parameters, nil)
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

func (s *service) checkControls(_ *context.T, _ rpc.ServerCall, args map[string]string) (bool, error) {
	for control, required := range s.controls {
		if required && args[control.String()] == "" {
			return false, errors.New("missing required argument: " + control.String())
		}
	}
	return true, nil
}
