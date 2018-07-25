// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package grail contains the Init function that all programs are expected to
// call.
package grail

import (
	"flag"
	"os"
	"sync"

	"github.com/google/gops/agent"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/pprof"
	"v.io/x/lib/vlog"
)

// Shutdown is a function that needs to be called to perform the final
// cleanup.
type Shutdown func()

var (
	initialized      = false
	mu               = sync.Mutex{}
	shutdownHandlers = []Shutdown{}
	gopsFlag         = flag.Bool("gops", false, "enable the gops listener")
)

// Init should be called once at the beginning at each executable that doesn't
// use the github.com/grailbio/base/cmdutil. The Shutdown function should be called to
// perform the final cleanup (closing logs for example).
//
// Note that this function will call flag.Parse().
//
// Suggested use:
//
//   shutdown := grail.Init()
//   defer shutdown()
func Init() Shutdown {
	mu.Lock()
	if initialized {
		panic("Init called twice")
	}
	initialized = true
	mu.Unlock()
	flag.CommandLine.Init(os.Args[0], flag.ContinueOnError)
	err := flag.CommandLine.Parse(os.Args[1:])
	if err == flag.ErrHelp {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}
	vlog.ConfigureLibraryLoggerFromFlags()
	log.SetOutputter(vlogOutputter{})
	pprof.Start()
	_, ok := os.LookupEnv("GOPS")
	if ok || *gopsFlag {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Print(err)
		}
	}
	return func() {
		RunShutdownCallbacks()
		pprof.Write(1)
		vlog.FlushLog()
	}
}

// RegisterShutdownCallback registers a function to be run in the Init shutdown
// callback. The callbacks will run in the reverse order of registration.
func RegisterShutdownCallback(cb Shutdown) {
	mu.Lock()
	shutdownHandlers = append(shutdownHandlers, cb)
	mu.Unlock()
}

// RunShutdownCallbacks run callbacks added in RegisterShutdownCallbacks. This
// function is not for general use.
func RunShutdownCallbacks() {
	mu.Lock()
	cbs := shutdownHandlers
	shutdownHandlers = nil
	mu.Unlock()
	for i := len(cbs) - 1; i >= 0; i-- {
		cbs[i]()
	}
}

type vlogOutputter struct{}

func (vlogOutputter) Level() log.Level {
	if vlog.V(1) {
		return log.Debug
	} else {
		return log.Info
	}
}

func (vlogOutputter) Output(calldepth int, level log.Level, s string) error {
	switch level {
	case log.Off:
	case log.Error:
		vlog.ErrorDepth(calldepth+1, s)
	case log.Info:
		vlog.InfoDepth(calldepth+1, s)
	default:
		vlog.VI(vlog.Level(level)).InfoDepth(calldepth+1, s)
	}
	return nil
}
