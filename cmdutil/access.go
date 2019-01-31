// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cmdutil

import (
	"fmt"
	"io"
	"os"
	"time"

	v23 "v.io/v23"
	"v.io/v23/context"
	"v.io/x/lib/cmdline"
)

// WriteBlessings will write the current principal and blessings to the
// supplied io.Writer.
func WriteBlessings(ctx *context.T, out io.Writer) {
	// Mimic the principal dump output.
	principal := v23.GetPrincipal(ctx)
	fmt.Fprintf(out, "Public key: %s\n", principal.PublicKey())
	fmt.Fprintf(out, "---------------- BlessingStore ----------------")
	fmt.Fprintf(out, principal.BlessingStore().DebugString())
	fmt.Fprintf(out, "---------------- BlessingRoots ----------------")
	fmt.Fprintf(out, principal.Roots().DebugString())
}

// CheckAccess checkes that the current process has credentials that
// will be valid for at least another 30 minutes. It is intended to
// allow for more useful and obvious error reporting.
func CheckAccess(ctx *context.T) (time.Duration, error) {
	if principal := v23.GetPrincipal(ctx); principal != nil {
		// We have access to some credentials so we'll try to load them.
		_, err := v23.WithPrincipal(ctx, principal)
		if err != nil {
			return 0, err
		}
		blessings, _ := principal.BlessingStore().Default()
		now := time.Now()
		left := blessings.Expiry().Sub(now)
		if blessings.Expiry().After(now.Add(30 * time.Minute)) {
			return left, nil
		}
		if blessings.Expiry().IsZero() {
			return left, fmt.Errorf("credentials are not set, try setting the V23_CREDENTIALS using 'export V23_CREDENTIALS=%s'", os.ExpandEnv("${HOME}/.v23"))
		}
		return left, fmt.Errorf("credentials are set to expire in %v, use grail-access to refresh them", left)
	}
	return 0, fmt.Errorf("credentials directory doesn't exist, use the grail-access and/or grail-role commands to create one and to login")
}

type runner struct {
	access bool
	ctxfn  func() *context.T
	run    func(*context.T, *cmdline.Env, []string) error
}

// Run implements cmdline.Runner.
func (r runner) Run(env *cmdline.Env, args []string) error {
	ctx := r.ctxfn()
	if os.Getenv("GRAIL_CMDUTIL_NO_ACCESS_CHECK") != "1" && r.access {
		if _, err := CheckAccess(ctx); err != nil {
			return err
		}
	}
	return r.run(ctx, env, args)
}

// V23RunnerFunc is like cmdutil.RunnerFunc, but allows for a context.T
// parameter that is given the context as obtained from ctxfn.
func V23RunnerFunc(ctxfn func() *context.T, run func(*context.T, *cmdline.Env, []string) error) cmdline.Runner {
	return RunnerFunc(runner{false, ctxfn, run}.Run)
}

// RunnerFuncWithAccessCheck is like V23RunnerFunc, but also calls CheckAccess
// to test for credential existence/expiry.
func RunnerFuncWithAccessCheck(ctxfn func() *context.T, run func(*context.T, *cmdline.Env, []string) error) cmdline.Runner {
	return RunnerFunc(runner{true, ctxfn, run}.Run)
}
