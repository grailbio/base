// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cmdutil

import (
	"fmt"
	"runtime"

	"v.io/v23/context"
	"v.io/x/lib/cmdline"
	"v.io/x/ref/lib/v23cmd"
)

var (
	version = "(missing)"
	tags    = ""
)

func init() {
	s := fmt.Sprintf("os=%s; arch=%s; %s", runtime.GOOS, runtime.GOARCH, runtime.Version())
	if tags == "" {
		tags = s
		return
	}

	tags = tags + "; " + s
}

func printVersion(prefix string) {
	fmt.Printf("%s/%v (%v)\n", prefix, version, tags)
}

// CreateVersionCommand creates a cmdline 'subcommand' to display version
// information.
//
// The format of the information printed is:
//
//    <prefix>/<version> (<tag1>; <tag2>; ...)
//
// The version and tags are set at build time using something like:
//
//    go build -ldflags \
//     "-X github.com/grailbio/base/cmdutil.version=$version \
//      -X github.com/grailbio/base/cmdutil.tags=$tags"
func CreateVersionCommand(name, prefix string) *cmdline.Command {
	return &cmdline.Command{
		Runner: RunnerFunc(func(_ *cmdline.Env, _ []string) error {
			printVersion(prefix)
			return nil
		}),
		Name:  name,
		Short: "Display version information",
	}
}

// CreateInfoCommand creates a command akin to that created by
// CreatedVersionCommand but also includes the output of running each of
// the supplied info closures.
func CreateInfoCommand(name, prefix string, info ...func(*context.T, *cmdline.Env, []string) error) *cmdline.Command {
	return &cmdline.Command{
		Runner: v23cmd.RunnerFunc(func(ctx *context.T, env *cmdline.Env, args []string) error {
			printVersion(prefix)
			for _, ifn := range info {
				if err := ifn(ctx, env, args); err != nil {
					return err
				}
			}
			return nil
		}),
		Name:  name,
		Short: "Display version information",
	}
}
