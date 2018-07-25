// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package awssession

import (
	"testing"

	"github.com/grailbio/base/security/ticket"
)

func TestProviderRetrieve(t *testing.T) {
	noError := false
	withError := true
	notExpired := false
	isExpired := true

	cases := []struct {
		description    string
		ticket         ticket.Ticket
		wantErr        bool
		wantExpiration bool
	}{
		{"nil", nil, withError, isExpired},
		{"empty AWS ticket", ticket.TicketAwsTicket{}, noError, notExpired},
		{"empty expiration", ticket.TicketAwsTicket{
			Value: ticket.AwsTicket{
				AwsCredentials: ticket.AwsCredentials{
					Expiration: "",
				},
			},
		}, noError, notExpired},
		{"bad expiration", ticket.TicketAwsTicket{
			Value: ticket.AwsTicket{
				AwsCredentials: ticket.AwsCredentials{
					Expiration: "bad-expiration-text",
				},
			},
		}, withError, isExpired},
		{"valid expiration", ticket.TicketAwsTicket{
			Value: ticket.AwsTicket{
				AwsCredentials: ticket.AwsCredentials{
					Expiration: "2018-01-01T00:00:00Z",
				},
			},
		}, noError, isExpired},
		{"valid non-expired expiration", ticket.TicketAwsTicket{
			Value: ticket.AwsTicket{
				AwsCredentials: ticket.AwsCredentials{
					Expiration: "2100-01-01T00:00:00Z",
				},
			},
		}, noError, notExpired},
	}

	for _, c := range cases {
		p := Provider{Ticket: c.ticket}
		_, gotErr := p.retrieve()
		if gotErr != nil && !c.wantErr {
			t.Errorf("%+v: got error %q, want error %v", c.description, gotErr, c.wantErr)
		}
		if gotExpiration := p.IsExpired(); gotExpiration != c.wantExpiration {
			t.Errorf("%+v: got IsExpired() %v, want %v", c.description, gotExpiration, c.wantExpiration)
		}
	}
}
