// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package dump

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/grailbio/base/log"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
)

// DefaultRegistry is a default registry that has this process's GUID as its ID.
var DefaultRegistry = NewRegistry(readExec())

// Register registers a new part to be included in the dump of the
// DefaultRegistry. name will become the filename of the part file in the dump
// tarball. f will be called to produce the contents of that file.
func Register(name string, f Func) {
	DefaultRegistry.Register(name, f)
}

// WriteDump writes a dump of the default registry.
func WriteDump(ctx context.Context, pfx string, w io.Writer) {
	DefaultRegistry.WriteDump(ctx, pfx, w)
}

// Name returns the name of the default registry. See (*Registry).Name.
func Name() string {
	return DefaultRegistry.Name()
}

// readExec returns a sanitized version of the executable name, if it can be
// determined. If not, returns "unknown".
func readExec() string {
	const unknown = "unknown"
	execPath, err := os.Executable()
	if err != nil {
		return unknown
	}
	rawExec := filepath.Base(execPath)
	var sanitized strings.Builder
	for _, r := range rawExec {
		if (r == '-' || 'a' <= r && r <= 'z') || ('0' <= r && r <= '9') {
			sanitized.WriteRune(r)
		}
	}
	if sanitized.Len() == 0 {
		return unknown
	}
	return sanitized.String()
}

// shellQuote quotes a string to be used as an argument in an sh command line.
func shellQuote(s string) string {
	// We wrap with single quotes, as they will work with any string except
	// those with single quotes. We handle single quotes by tranforming them
	// into "'\''" and letting the shell concatenate the strings back together.
	return "'" + strings.Replace(s, "'", `'\''`, -1) + "'"
}

// dumpCmdline writes the command-line of the current execution. It writes it
// in a format that can be directly pasted into sh to be run.
func dumpCmdline(ctx context.Context, w io.Writer) error {
	args := make([]string, len(os.Args))
	for i := range args {
		args[i] = shellQuote(os.Args[i])
	}
	_, err := io.WriteString(w, strings.Join(args, " "))
	return err
}

func dumpCpuinfo(ctx context.Context, w io.Writer) error {
	info, err := cpu.InfoWithContext(ctx)
	if err != nil {
		return fmt.Errorf("error getting cpuinfo: %v", err)
	}
	s, err := json.MarshalIndent(info, "", "    ")
	if err != nil {
		return fmt.Errorf("error marshaling cpuinfo: %v", err)
	}
	_, err = w.Write(s)
	return err
}

func dumpLoadinfo(ctx context.Context, w io.Writer) error {
	type loadinfo struct {
		Avg  *load.AvgStat  `json:"average"`
		Misc *load.MiscStat `json:"miscellaneous"`
	}
	var info loadinfo
	avg, err := load.AvgWithContext(ctx)
	if err != nil {
		return fmt.Errorf("error getting load averages: %v", err)
	}
	info.Avg = avg
	misc, err := load.MiscWithContext(ctx)
	if err != nil {
		return fmt.Errorf("error getting miscellaneous load stats: %v", err)
	}
	info.Misc = misc
	s, err := json.MarshalIndent(info, "", "    ")
	if err != nil {
		return fmt.Errorf("error marshaling loadinfo: %v", err)
	}
	_, err = w.Write(s)
	return err
}

func dumpMeminfo(ctx context.Context, w io.Writer) error {
	type meminfo struct {
		Virtual *mem.VirtualMemoryStat `json:"virtualMemory"`
		Runtime runtime.MemStats       `json:"goRuntime"`
	}
	var info meminfo
	vmem, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return fmt.Errorf("error getting virtual memory stats: %v", err)
	}
	info.Virtual = vmem
	runtime.ReadMemStats(&info.Runtime)
	s, err := json.MarshalIndent(info, "", "    ")
	if err != nil {
		return fmt.Errorf("error marshaling meminfo: %v", err)
	}
	_, err = w.Write(s)
	if err != nil {
		return fmt.Errorf("error writing memory stats: %v", err)
	}
	return nil
}

// dumpGoroutine writes current goroutines with human-readable source
// locations.
func dumpGoroutine(ctx context.Context, w io.Writer) error {
	p := pprof.Lookup("goroutine")
	if p == nil {
		panic("no goroutine profile")
	}
	// debug == 2 prints goroutine stacks in the same form as that printed for
	// an unrecovered panic.
	return p.WriteTo(w, 2)
}

// dumpPprofHeap writes a pprof heap profile.
func dumpPprofHeap(ctx context.Context, w io.Writer) error {
	p := pprof.Lookup("heap")
	if p == nil {
		panic("no heap profile")
	}
	return p.WriteTo(w, 0)
}

// dumpPprofMutex writes a fraction of the stack traces of goroutines with
// contended mutexes.
func dumpPprofMutex(ctx context.Context, w io.Writer) error {
	p := pprof.Lookup("mutex")
	if p == nil {
		panic("no mutex profile")
	}
	// debug == 1 makes use function names instead of hexadecimal addresses, so
	// it can also be human-readable.
	return p.WriteTo(w, 1)
}

// dumpPprofHeap writes a pprof CPU profile sampled for 30 seconds or until the
// context is done, whichever is shorter.
func dumpPprofProfile(ctx context.Context, w io.Writer) error {
	if err := pprof.StartCPUProfile(w); err != nil {
		return err
	}
	startTime := time.Now()
	defer pprof.StopCPUProfile()
	select {
	case <-time.After(30 * time.Second):
	case <-ctx.Done():
		d := time.Since(startTime)
		log.Printf("dump: CPU profile cut short to %s", d.String())
	}
	return nil
}

// dumpVars writes public variables exported by the expvar package. The output
// is equivalent to the output of the "/debug/vars" endpoint.
func dumpVars(ctx context.Context, w io.Writer) error {
	if _, err := fmt.Fprintf(w, "{\n"); err != nil {
		return err
	}
	var (
		err   error
		first = true
	)
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			if _, err = fmt.Fprintf(w, ",\n"); err != nil {
				return
			}
		}
		first = false
		if _, err = fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value); err != nil {
			return
		}
	})
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "\n}\n"); err != nil {
		return err
	}
	return nil
}

// Func is the type of a function that is registered in (*Registry).Register to
