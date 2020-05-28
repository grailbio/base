// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"io/ioutil"
	"net/http"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/security/identity"
	"v.io/v23/context"
	"v.io/v23/security"
)

func fetchEC2Blessings(ctx *context.T) (security.Blessings, error) {
	stub := identity.Ec2BlesserClient(blesserEc2Flag)
	client := http.Client{
		Timeout: 5 * time.Second,
	}
	resp, err := client.Get(ec2InstanceIdentityFlag)
	if err != nil {
		return security.Blessings{}, errors.E("unable to talk to the EC2 metadata server (not an EC2 instance?)", err)
	}
	identityDocument, err := ioutil.ReadAll(resp.Body)
	if err2 := resp.Body.Close(); err2 != nil {
		log.Print("warning: ", err2)
	}
	log.Debug.Printf("pkcs7: %d bytes", len(identityDocument))
	if err != nil {
		return security.Blessings{}, err
	}
	return stub.BlessEc2(ctx, string(identityDocument))
}
