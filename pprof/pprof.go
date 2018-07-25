// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package pprof

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	rtpprof "runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grailbio/base/grail/go/net/http/pprof"
	"v.io/x/lib/vlog"
)

// Profiling provides access to the various profiling options provided
// by runtime/pprof.
type profiling struct {
	started int32 // # calls to Start().

	cpuName    string
	heapName   string
	threadName string
	blockName  string
	mutexName  string

	blockRate       int
	mutexRate       int
	profileInterval float64

	mu *sync.Mutex
	// Following fields are guarded by mu.
	cpuFile    *os.File
	generation int // guarded by mu

	// HTTP Listen address, in form ":12345"
	httpAddr string
	// Actual listen address of the debug HTTP server.
	pprofAddr net.Addr
}

func newProfiling() *profiling {
	pr := &profiling{
		mu: &sync.Mutex{},
	}
	for _, p := range []struct {
		fl interface{}
		n  string
		dv interface{}
		d  string
	}{
		{&pr.httpAddr, "pprof", "", "address for pprof server"},
		{&pr.cpuName, "cpu-profile", "", "filename for cpu profile"},
		{&pr.heapName, "heap-profile", "", "filename prefix for heap profiles"},
		{&pr.threadName, "thread-create-profile", "", "filename prefix for thread create profiles"},
		{&pr.blockName, "block-profile", "", "filename prefix for block profiles"},
		{&pr.mutexName, "mutex-profile", "", "filename prefix for mutex profiles"},
		{&pr.mutexRate, "mutex-profile-rate", 200, "rate for runtime.SetMutexProfileFraction"},
		{&pr.blockRate, "block-profile-rate", 200, "rate for runtime.SetBlockProfileRate"},
		{&pr.profileInterval, "profile-interval-s", 0.0, "If >0, output new profiles at this interval (seconds). If <=0, profiles are written only when Write() is called"},
	} {
		fn := p.n
		switch flt := p.fl.(type) {
		case *string:
			flag.StringVar(flt, fn, p.dv.(string), p.d)
		case *int:
			flag.IntVar(flt, fn, p.dv.(int), p.d)
		case *float64:
			flag.Float64Var(flt, fn, p.dv.(float64), p.d)
		}
	}
	return pr
}

func generationSuffix(generation int) string {
	return fmt.Sprintf("-%05v.pprof", generation)
}

// Write writes the profile information to new files. Each call results
// in a new file name of the for <command-line-prefix>-<number> where
// number is incremented each time Write is called. All of the profiles
// enabled on the command line are written.
func (p *profiling) Write(debug int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	suffix := generationSuffix(p.generation)
	p.generation++
	for _, n := range []struct {
		fl string
		pn string
	}{
		{p.cpuName, "cpu"},
		{p.heapName, "heap"},
		{p.threadName, "threadcreate"},
		{p.blockName, "block"},
		{p.mutexName, "mutex"},
	} {
		if len(n.fl) == 0 {
			continue
		}
		if n.fl == p.cpuName {
			p.stopCPU()
			p.startCPU()
		} else {
			pr := rtpprof.Lookup(n.pn)
			if pr == nil {
				vlog.Errorf("failed to lookup profile: %v", n.pn)
			}
			fn := n.fl + suffix
			f, err := os.Create(fn)
			if err != nil {
				vlog.Errorf("failed to create %v for %v profile", fn, n.pn)
				continue
			}
			defer f.Close()
			if err := pr.WriteTo(f, debug); err != nil {
				vlog.Errorf("failed to write Profiling for %v to %v: %v", n.pn, fn, err)
			}
			vlog.VI(1).Infof("%v: Wrote profile", f.Name())
		}
	}

}

// Start starts CPU and all other profiling that has been specified on the
// command line.  Calling this method multiple times is a noop.
func (p *profiling) Start() {
	if atomic.AddInt32(&p.started, 1) > 1 {
		return
	}
	if len(p.blockName) > 0 || len(p.httpAddr) > 0 {
		runtime.SetBlockProfileRate(p.blockRate)
	}
	if len(p.mutexName) > 0 || len(p.httpAddr) > 0 {
		runtime.SetMutexProfileFraction(p.mutexRate)
	}
	if len(p.cpuName) > 0 {
		p.startCPU()
	}
	if p.profileInterval > 0 {
		go func() {
			for {
				time.Sleep(time.Duration(p.profileInterval * float64(time.Second)))
				p.Write(1)
			}
		}()
	}
	if len(p.httpAddr) > 0 {
		mux := http.NewServeMux()
		mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write([]byte("<ul><li> <a href=\"/debug/pprof/goroutine?debug=1\">threadz</a></ul>"))
		}))
		mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
		mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
		mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
		mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
		mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))

		l, err := net.Listen("tcp", p.httpAddr)
		if err != nil {
			vlog.Error(err)
			return
		}

		p.pprofAddr = l.Addr()
		vlog.Infof("Pprof server listening on %s", p.pprofAddr.String())
		go func() {
			if err := http.Serve(l, mux); err != nil {
				vlog.Error(err)
			}
		}()
	}

}

// startCPU starts CPU profiling. The CPU profiler must not be already active.
func (p *profiling) startCPU() {
	f, err := os.Create(p.cpuName + generationSuffix(p.generation))
	if err != nil {
		vlog.Fatal("could not create CPU profile: ", err)
	}
	p.cpuFile = f
	if err := rtpprof.StartCPUProfile(f); err != nil {
		f.Close()
		vlog.Fatal("could not start CPU profile: ", err)
	}
}

// stopCPU stops cpu profiling and must be called to ensure that the CPU
// Profiling information is written to its output file.
func (p *profiling) stopCPU() {
	rtpprof.StopCPUProfile()
	p.cpuFile.Close()
	p.cpuFile = nil
}

var singleton *profiling

func init() {
	singleton = newProfiling()
}

// Start starts the cpu, memory, and other profilers specified in the
// commandline. This function has no effect if none of the following flags is
// set.
//
// -cpu-profile
// -heap-profile
// -thread-create-profile
// -block-profile
// -mutex-profile
// -profile-interval-s
// -pprof
//
// This function should be called at the start of a process. Calling it multiple
// times a noop.
func Start() {
	singleton.Start()
}

// PprofAddr returns the listen address of the HTTP httpserver. It is useful
// when the process was started with flag -pprof=:0.
func HTTPAddr() net.Addr {
	return singleton.pprofAddr
}

// WritePprof writes the profile information to new files. Each call results in
// a new file name of the for <command-line-prefix>-<number> where number is
// incremented each time Write is called. All of the profiles enabled on the
// command line are written.
func Write(debug int) {
	singleton.Write(debug)
}
