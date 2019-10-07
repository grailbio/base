// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package grail contains the Init function that all programs are expected to
// call.
package grail

import (
	"flag"
	"os"
	"strings"
	"sync"

	"github.com/google/gops/agent"
	"github.com/grailbio/base/config"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/pprof"
	"github.com/grailbio/base/shutdown"

	// GRAIL applications require the AWS ticket provider.
	_ "github.com/grailbio/base/config/awsticket"
	"v.io/x/lib/vlog"
)

// Shutdown is a function that needs to be called to perform the final
// cleanup.
type Shutdown func()

var (
	initialized      = false
	mu               = sync.Mutex{}
	gopsFlag         = flag.Bool("gops", false, "enable the gops listener")
)

// Init should be called once at the beginning at each executable that doesn't
// use the github.com/grailbio/base/cmdutil. The Shutdown function should be called to
// perform the final cleanup (closing logs for example).
//
// Init also applies a default configuration profile (see package
// github.com/grailbio/base/config), and adds profile flags to the
// default flag set. The default profile path used is $HOME/grail/profile.
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

	profile := config.New()
	config.NewDefault = func() *config.Profile {
		if err := profile.Parse(strings.NewReader(defaultProfile)); err != nil {
			panic("grail: error in default profile: " + err.Error())
		}
		if err := profile.ProcessFlags(); err != nil {
			log.Fatal(err)
		}
		return profile
	}
	profile.RegisterFlags(flag.CommandLine, "", os.ExpandEnv("$HOME/grail/profile"))
	flag.CommandLine.Init(os.Args[0], flag.ContinueOnError)
	err := flag.CommandLine.Parse(os.Args[1:])
	if err == flag.ErrHelp {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	}
	if err := vlog.ConfigureLibraryLoggerFromFlags(); err != nil {
		vlog.Error(err)
	}
	log.SetOutputter(vlogOutputter{})
	if profile.NeedProcessFlags() {
		_ = config.Application()
	}

	pprof.Start()
	_, ok := os.LookupEnv("GOPS")
	if ok || *gopsFlag {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Print(err)
		}
	}
	return func() {
		shutdown.Run()
		pprof.Write(1)
		vlog.FlushLog()
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
