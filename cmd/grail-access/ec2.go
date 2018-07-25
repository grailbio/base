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

	"github.com/grailbio/base/grail/data/v23data"
	"github.com/grailbio/base/security/identity"
	"v.io/v23"
	"v.io/v23/context"
	"v.io/v23/security"
	"v.io/x/lib/cmdline"
	"v.io/x/lib/vlog"
	libsecurity "v.io/x/ref/lib/security"
)

const instanceIdentityURL = "http://169.254.169.254/latest/dynamic/instance-identity/pkcs7"

func runEc2(ctx *context.T, env *cmdline.Env, args []string) error {
	// TODO(razvanm): do we need to kill the v23agentd?

	// Best-effort cleanup.
	os.RemoveAll(credentialsDirFlag)

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
	doc := identityDocumentFlag
	if doc == "" {
		client := http.Client{
			Timeout: time.Duration(5 * time.Second),
		}
		resp, err := client.Get(instanceIdentityURL)
		if err != nil {
			vlog.Error(err)
			return fmt.Errorf("unable to talk to the EC2 metadata server (not an EC2 instance?)")
		}
		b, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		vlog.VI(1).Infof("pkcs7: %d bytes", len(b))
		if err != nil {
			return err
		}
		doc = string(b)
	}
	blessings, err := stub.BlessEc2(ctx, doc)
	if err != nil {
		vlog.Error(err)
		return err
	}

	principal = v23.GetPrincipal(ctx)
	principal.BlessingStore().SetDefault(blessings)
	principal.BlessingStore().Set(blessings, security.AllPrincipals)
	if err := security.AddToRoots(principal, blessings); err != nil {
		vlog.Error(err)
		return fmt.Errorf("failed to add blessings to recognized roots: %v", err)
	}

	if err := v23data.InjectPipelineBlessings(ctx); err != nil {
		vlog.Error(err)
		return fmt.Errorf("failed to add the pipeline roots")
	}

	dump(ctx, env)

	return nil
}
