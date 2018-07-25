// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package ticket

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/grailbio/base/security/keycrypt"
	"v.io/x/lib/vlog"
)

const (
	b2AuthorizeURL = "https://api.backblazeb2.com/b2api/v1/b2_authorize_account"
)

func (b *B2AccountAuthorizationBuilder) newB2Ticket() (TicketB2Ticket, error) {
	vlog.Infof("B2AccountAuthorizationBuilder: %+v", b)

	b2Ticket, err := b.genB2Ticket()
	if err != nil {
		return TicketB2Ticket{}, err
	}

	return TicketB2Ticket{
		Value: *b2Ticket,
	}, nil
}

func (b *B2AccountAuthorizationBuilder) genB2Ticket() (*B2Ticket, error) {
	secret, err := keycrypt.Lookup(b.ApplicationKey)
	if err != nil {
		return nil, err
	}

	applicationKey, err := secret.Get()
	if err != nil {
		return nil, err
	}

	headerForAuthorizeAccount := "Basic " + base64.StdEncoding.EncodeToString([]byte(b.AccountId+":"+string(applicationKey)))

	req, err := http.NewRequest("GET", b2AuthorizeURL, nil)
	if err != nil {
		vlog.Errorf("Cannot create new request: %s (%s)", b2AuthorizeURL, err)
		return nil, err
	}
	req.Header.Set("Authorization", headerForAuthorizeAccount)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		vlog.Errorf("Cannot authorize: %s (%s)", b2AuthorizeURL, err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		type ErrorResponse struct {
			Status  int
			Code    string
			Message string
		}
		var er ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&er); err != nil {
			vlog.Errorf("Cannot decode: %v (%s)", er, err)
			return nil, err
		}
		err := fmt.Errorf("Status %d: %s", er.Status, er.Message)
		vlog.Error(err)
		return nil, err
	}

	var b2Ticket B2Ticket
	if err := json.NewDecoder(resp.Body).Decode(&b2Ticket); err != nil {
		vlog.Errorf("Cannot decode: %v (%s)", b2Ticket, err)
		return nil, err
	}
	return &b2Ticket, nil
}
