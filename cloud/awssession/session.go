// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package awssession provides a simple way to obtain AWS session.Session
// using GRAIL tickets.
package awssession

import (
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"v.io/v23/context"
)

const (
	region          = "us-west-2"
	grailTicketPath = "GRAIL_AWS_TICKET_PATH"
	defaultTimeout  = 10 * time.Second
)

// NewWithTicket creates a AWS session using a GRAIL ticket. This is helper
// that uses inside a Provider with a timeout of 10 seconds. The region will be
// set to 'us-west-2' and can be overriden by passing an appropriate
// *aws.Config.
func NewWithTicket(ctx *context.T, ticketPath string, cfgs ...*aws.Config) (*session.Session, error) {
	creds := credentials.NewCredentials(&Provider{
		Ctx:        ctx,
		Timeout:    defaultTimeout,
		TicketPath: ticketPath,
	})
	cfg := aws.NewConfig().WithCredentials(creds).WithRegion(region)
	cfgs = append([]*aws.Config{cfg}, cfgs...)
	return session.NewSession(cfgs...)
}

// NewWithProvider creates a AWS session using a provider. The region will be
// set to 'us-west-2' and can be overriden by passing an appropriate
// *aws.Config.
func NewWithProvider(provider credentials.Provider, cfgs ...*aws.Config) (*session.Session, error) {
	creds := credentials.NewCredentials(provider)
	cfg := aws.NewConfig().WithCredentials(creds).WithRegion(region)
	cfgs = append([]*aws.Config{cfg}, cfgs...)
	return session.NewSession(cfgs...)
}

// New creates a new AWS session using a GRAIL ticket indicated by the
// GRAIL_AWS_TICKET_PATH env variable if the variable is defined or by the
// default AWS session otherwise. The region will be set to 'us-west-2' and can
// be overriden by passing an appropriate *aws.Config.
func New(ctx *context.T, cfgs ...*aws.Config) (*session.Session, error) {
	ticketPath := os.Getenv(grailTicketPath)
	if ticketPath != "" {
		return NewWithTicket(ctx, ticketPath, cfgs...)
	}
	cfgs = append([]*aws.Config{aws.NewConfig().WithRegion(region)}, cfgs...)
	return session.NewSession(cfgs...)
}
