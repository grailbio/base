// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package dump provides the endpoint "debug/dump", registered with
// http.DefaultServeMux, which returns a dump of useful diagnostic information
// as a tarball. The base configuration includes several useful diagnostics
// (see init). You may also register your own dump parts to be included, e.g.:
//
//  Register("mystuff", func(ctx context.Context, w io.Writer) error {
//      // Write to w.
//      return nil
//  })
//
// The endpoint responds with a gzipped tarball. The Content-Disposition of the
// response suggests a pseudo-unique filename to make it easier to deal with
// multiple dumps. Use curl flags to accept the suggested filename
// (recommended).
//
//  curl -OJ http://example:1234/debug/dump
//
// Note that it will take at least 30 seconds to respond, as some of the parts
// of the base configuration are 30-second profiles.
package dump

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
)

// init registers commonly useful parts in the DefaultRegistry and configures
// http.DefaultServeMux with the endpoint "/debug/dump" for getting the dump.
func init() {
	Register("cmdline", dumpCmdline)
	Register("cpuinfo", dumpCpuinfo)
	Register("loadinfo", dumpLoadinfo)
	Register("meminfo", dumpMeminfo)
	Register("pprof-goroutine", dumpGoroutine)
	Register("pprof-heap", dumpPprofHeap)
	Register("pprof-mutex", dumpPprofMutex)
	Register("pprof-profile", dumpPprofProfile)
	Register("vars", dumpVars)
	http.Handle("/debug/dump", DefaultRegistry)
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

// Func is the type of the function that is registered in (*Registry).Register
// to be called when producing a dump.
type Func func(ctx context.Context, w io.Writer) error

// part is one part of a dump. It is ultimately expressed as a single file that
// is part the tarball archive dump.
type part struct {
	// name is the name of this part of the dump. It is used as the filename in
	// the dump tarball.
	name string
	// f is called to produce the contents of this part of the dump.
	f Func
}

// Registry maintains the set of parts that will compose the dump.
type Registry struct {
	mu sync.Mutex
	// id is the identifier of this registry, which eventually becomes part of
	// the suggested filename for the dump.
	id    string
	parts []part
}

// DefaultRegistry is a default registry that uses a randomly generated ID.
var DefaultRegistry = NewRegistry()

// Register registers a new part to be included in the dump of the
// DefaultRegistry. name will become the filename of the part file in the dump
// tarball. f will be called to produce the contents of that file.
func Register(name string, f Func) {
	DefaultRegistry.Register(name, f)
}

// idRand is used to generate random IDs for registries.
var idRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func randID() string {
	const N = 5
	alphabet := []rune("0123456789abcdefghijklmnopqrstuvwxyz")
	rs := make([]rune, N)
	for i := range rs {
		rs[i] = alphabet[idRand.Intn(len(alphabet))]
	}
	return string(rs)
}

// NewRegistry returns a new registry for the parts to be included in the dump.
func NewRegistry() *Registry {
	return &Registry{id: randID()}
}

// Register registers a new part to be included in the dump of reg. name will
// become the filename of the part file in the dump tarball. f will be called
// to produce the contents of that file.
func (reg *Registry) Register(name string, f Func) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	for _, part := range reg.parts {
		if part.name == name {
			panic(fmt.Sprintf("duplicate part name %q", name))
		}
	}
	reg.parts = append(reg.parts, part{name: name, f: f})
}

// result is by worker goroutines to communicate results back to the main
// dumping thread. Only one of err and file will be non-nil.
type result struct {
	// part is the part to which this result applies.
	part part
	// err will be non-nil if there was an error producing the file of the part
	// of the dump.
	err error
	// file will be non-nil in a successful result and will be the file that
	// will be included in the dump tarball.
	file *os.File
}

// processPart is called by worker goroutines to process a single part.
func processPart(ctx context.Context, part part) result {
	tmpfile, err := ioutil.TempFile("", "dump")
	if err != nil {
		return result{
			part: part,
			err:  fmt.Errorf("error creating temp file: %v", err),
		}
	}
	if err := os.Remove(tmpfile.Name()); err != nil {
		log.Printf("dump: error removing temp file %s: %v", tmpfile.Name(), err)
	}
	if err := part.f(ctx, tmpfile); err != nil {
		return result{
			part: part,
			err:  fmt.Errorf("error writing part contents: %v", err),
		}
	}
	if _, err := tmpfile.Seek(0, 0); err != nil {
		return result{
			part: part,
			err:  fmt.Errorf("error seeking to read temp file for dump: %v", err),
		}
	}
	return result{part: part, file: tmpfile}
}

// writeTar writes a file to tw with filename name.
func writeTar(name string, f *os.File, tw *tar.Writer) error {
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("error getting file stat of %q: %v", f.Name(), err)
	}
	hdr := &tar.Header{
		Name:    name,
		Mode:    0600,
		Size:    fi.Size(),
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("error writing tar header in diagnostic dump: %v", err)
	}
	if _, err = io.Copy(tw, f); err != nil {
		return fmt.Errorf("error writing diagnostic dump: %v", err)
	}
	return nil
}

// writeResult writes a single result to tw. pfx is the path that will be
// prepended to the part name to construct the full path of the entry in the
// archive.
func writeResult(pfx string, r result, tw *tar.Writer) (err error) {
	if r.err != nil {
		return fmt.Errorf("error dumping %s: %v", r.part.name, r.err)
	}
	defer func() {
		closeErr := r.file.Close()
		if err == nil {
			err = fmt.Errorf("error closing temp file %q: %v", r.file.Name(), closeErr)
		}
	}()
	if tarErr := writeTar(pfx+"/"+r.part.name, r.file, tw); tarErr != nil {
		return fmt.Errorf("error writing %s to archive: %v", r.part.name, tarErr)
	}
	return nil
}

// writeDump writes the dump to w. pfx is prepended to the names of the parts
// of the dump, e.g. if pfx == "dump-123" and part name == "cpu",
// "dump-123/cpu" will be written into the archive.
func (reg *Registry) writeDump(ctx context.Context, pfx string, w io.Writer) {
	gzw := gzip.NewWriter(w)
	defer func() {
		if err := gzw.Close(); err != nil {
			log.Error.Printf("dump: error closing dump gzip writer: %v", err)
		}
	}()
	tw := tar.NewWriter(gzw)
	defer func() {
		if err := tw.Close(); err != nil {
			log.Error.Printf("dump: error closing dump tar writer: %v", err)
		}
	}()
	reg.mu.Lock()
	// Snapshot reg.parts to release the lock quickly.
	parts := reg.parts
	reg.mu.Unlock()
	const concurrency = 8
	var wg sync.WaitGroup
	resultC := make(chan result, concurrency)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range resultC {
			if err := writeResult(pfx, r, tw); err != nil {
				// We only log errors because we try to produce a partial dump
				// even if some parts fail.
				log.Error.Printf("dump: %v", err)
			}
		}
	}()
	err := traverse.Limit(concurrency).Each(len(parts), func(i int) error {
		partCtx, partCtxCancel := context.WithTimeout(ctx, 2*time.Minute)
		result := processPart(partCtx, parts[i])
		partCtxCancel()
		resultC <- result
		return nil
	})
	if err != nil {
		log.Error.Printf("dump: error processing parts: %v", err)
		return
	}
	close(resultC)
	wg.Wait()
}

// ServeHTTP serves the dump as a tarball, with a Content-Disposition set with
// a unique filename.
func (reg *Registry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/gzip")
	exec := "unknown"
	execPath, err := os.Executable()
	if err == nil {
		exec = filepath.Base(execPath)
	}
	timestamp := time.Now().Format("20060102T150405")
	// We try to be informative and unique by including the executable name, a
	// random ID for this execution, and the timestamp in the filename.
	pfx := fmt.Sprintf("%s-%s-%s", exec, timestamp, reg.id)
	filename := pfx + ".tar.gz"
	w.Header().Set("Content-Disposition", "attachment; filename="+filename)
	reg.writeDump(r.Context(), pfx, w)
}
