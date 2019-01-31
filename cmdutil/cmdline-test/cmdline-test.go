// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"

	"github.com/grailbio/base/cmdutil"
	_ "github.com/grailbio/base/cmdutil/interactive"
	"github.com/grailbio/base/vcontext"
	"v.io/v23/context"
	"v.io/x/lib/cmdline"
	"v.io/x/lib/vlog"
)

var cmdRoot = &cmdline.Command{
	Name: "cmdline-test",
	Children: []*cmdline.Command{
		logging,
		access,
	},
}

var logging = &cmdline.Command{
	Name:     "logging",
	ArgsName: "args",
	Runner:   cmdutil.RunnerFunc(runner),
}

var access = &cmdline.Command{
	Name:     "access",
	ArgsName: "args",
	Runner:   cmdutil.RunnerFuncWithAccessCheck(vcontext.Background, runnerWithRPC),
}

func main() {
	cmdline.Main(cmdRoot)
}

func runner(_ *cmdline.Env, args []string) error {
	fmt.Printf("%v\n", vlog.Log.LogDir())
	vlog.Infof("-----")
	for i, a := range args {
		vlog.Infof("T: %d: %v", i, a)
	}
	return nil
}

func runnerWithRPC(ctx *context.T, _ *cmdline.Env, args []string) error {
	fmt.Printf("%v\n", vlog.Log.LogDir())
	vlog.Infof("-----")
	for i, a := range args {
		vlog.Infof("T: %d: %v", i, a)
	}
	return nil
}
