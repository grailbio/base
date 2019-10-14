// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//+build !unit

package cmdutil_test

import (
	"flag"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grailbio/base/cmdutil"
	"github.com/grailbio/testutil"
	"v.io/x/ref/lib/security"
	_ "v.io/x/ref/runtime/factories/library"
	"v.io/x/ref/test"
)

func TestCheckAccess(t *testing.T) {
	flag.Set("v23.credentials", "")
	dir, cleanup := testutil.TempDir(t, "", "check-access")
	defer cleanup()
	cdir := filepath.Join(dir, "creds")
	ctx, shutdown := test.V23Init()
	defer shutdown()
	_, err := security.CreatePersistentPrincipal(cdir, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cmdutil.CheckAccess(ctx)
	if err == nil || !strings.Contains(err.Error(), "credentials are not set") {
		t.Fatalf("missing or wrong error: %v", err)
	}
	// TODO: test for blessings being current.
}
