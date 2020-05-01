// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/security/identity"
	v23 "v.io/v23"
	"v.io/v23/context"
	"v.io/v23/security"
	"v.io/x/lib/vlog"
	libsecurity "v.io/x/ref/lib/security"
)

func runEc2(ctx *context.T) error {
	// TODO(razvanm): do we need to kill the v23agentd?

	// Best-effort cleanup.
	err := os.RemoveAll(credentialsDirFlag)
	if !os.IsNotExist(err) {
		vlog.Error(err)
		// continue as best effort...
	}

	principal, err := libsecurity.CreatePersistentPrincipal(credentialsDirFlag, nil)
	if err != nil {
		vlog.Error(err)
		return err
	}

	ctx, err = v23.WithPrincipal(ctx, principal)
	if err != nil {
		vlog.Error(err)
		return err
	}

	stub := identity.Ec2BlesserClient(blesserEc2Flag)
	client := http.Client{
		Timeout: 5 * time.Second,
	}
	resp, err := client.Get(ec2InstanceIdentityFlag)
	if err != nil {
		vlog.Error(err)
		return fmt.Errorf("unable to talk to the EC2 metadata server (not an EC2 instance?)")
	}
	identityDocument, err := ioutil.ReadAll(resp.Body)
	if err2 := resp.Body.Close(); err2 != nil {
		vlog.Info("warning: ", err2)
	}
	vlog.VI(1).Infof("pkcs7: %d bytes", len(identityDocument))
	if err != nil {
		return err
	}
	blessings, err := stub.BlessEc2(ctx, string(identityDocument))
	if err != nil {
		vlog.Error(err)
		return err
	}

	principal = v23.GetPrincipal(ctx)
	if err := principal.BlessingStore().SetDefault(blessings); err != nil {
		vlog.Error(err)
		return errors.E(err, "set default blessings")
	}
	_, err = principal.BlessingStore().Set(blessings, security.AllPrincipals)
	if err != nil {
		vlog.Error(err)
		return errors.E(err, "set blessings")
	}
	if err := security.AddToRoots(principal, blessings); err != nil {
		vlog.Error(err)
		return errors.E(err, "add blessings to recognized root")
	}

	dump(ctx)

	return nil
}
