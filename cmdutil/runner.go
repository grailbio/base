// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cmdutil

import (
	"sync"

	"github.com/grailbio/base/pprof"
	"github.com/grailbio/base/shutdown"
	"v.io/x/lib/cmdline"
	"v.io/x/lib/vlog"
)

var runnerOnce sync.Once

// RunnerFunc is an adapter that turns regular functions into cmdline.Runners.
type RunnerFunc func(*cmdline.Env, []string) error

// Run implements the cmdline.Runner interface method by calling f(env, args)
// and also ensures that vlog logging is configured and that a flush is done
// at the end.
func (f RunnerFunc) Run(env *cmdline.Env, args []string) error {
	runnerOnce.Do(func() {
		vlog.ConfigureLibraryLoggerFromFlags()
		pprof.Start()
	})
	err := f(env, args)

	shutdown.Run()
	vlog.FlushLog()
	pprof.Write(1)
	return err
}
