// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/grailbio/base/cmdutil"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/pprof"
	"v.io/x/lib/cmdline"
	"v.io/x/lib/vlog"
)

func run(env *cmdline.Env, args []string) error {
	go func() {
		time.Sleep(time.Hour)
		log.Fatal("Timeout!")
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)

	srv := &http.Server{}
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return err
	}
	go func() {
		http.HandleFunc("/alive/", func(w http.ResponseWriter, r *http.Request) {
			vlog.Info("Alive!")
		})
		http.HandleFunc("/write/", func(w http.ResponseWriter, r *http.Request) {
			vlog.Info("Writing!")
			pprof.Write(1)
		})
		http.HandleFunc("/quitquitquit/", func(w http.ResponseWriter, r *http.Request) {
			vlog.Info("Done!")
			wg.Done()
		})
		vlog.Info("Starting Webserver")
		if err := srv.Serve(l); err != nil {
			vlog.Error(err)
		}
	}()
	fmt.Println(l.Addr().String())
	fmt.Println(pprof.HTTPAddr())
	wg.Wait()
	pprof.Write(1)
	return srv.Close()
}

func newCmdRoot() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:   "cmdhttp-test",
		Runner: cmdutil.RunnerFunc(run),
		Short:  "Test command for cmdhttp",
	}
	return cmd
}

func main() {
	cmdline.HideGlobalFlagsExcept()
	cmdline.Main(newCmdRoot())
}
