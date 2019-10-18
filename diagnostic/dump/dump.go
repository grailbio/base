// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package dump provides the endpoint "debug/dump", registered with
// http.DefaultServeMux, which returns a dump of useful diagnostic information
// as a tarball. The base configuration includes several useful diagnostics
// (see init). You may also register your own dump parts to be included, e.g.:
//
//  Register("mystuff", func(ctx context.Context, w io.Writer) error {
//      w.Write([]byte("mystuff diagnostic data"))
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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
)

// init registers commonly useful parts in the registry and configures
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

// part is one part of a dump. It is ultimately expressed as a single file that
// is part the tarball archive dump.
type part struct {
	// name is the name of this part of the dump. It is used as the filename in
	// the dump tarball.
	name string
	// f is called to produce the contents of this part of the dump.
	f Func
}

// Func is the function to be called when producing a dump for a part.
type Func func(ctx context.Context, w io.Writer) error

// Registry maintains the set of parts that will compose the dump.
type Registry struct {
	mu sync.Mutex
	// id is the identifier of this registry, which eventually becomes part of
	// the suggested filename for the dump.
	id    string
	parts []part

	// createTime is the time at which this Registry was created with
	// NewRegistry.
	createTime time.Time
}

// NewRegistry returns a new registry for the parts to be included in the dump.
func NewRegistry(id string) *Registry {
	return &Registry{id: id, createTime: time.Now()}
}

// Name returns a name for reg that is convenient for naming dump files, as it
// is pseudo-unique and includes the registry ID, the time at which the registry
// was created, and the duration from that creation time.
func (reg *Registry) Name() string {
	sinceCreate := time.Since(reg.createTime)
	ss := []string{reg.id, reg.createTime.Format(createTimeFormat), formatDuration(sinceCreate)}
	return strings.Join(ss, ".")
}

// Register registers a new part to be included in the dump of reg. Name will
// become the filename of the part file in the dump tarball. Func f will be
// called to produce the contents of that file.
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

// partFile is used by worker goroutines to communicate results back to the main
// dumping thread. Only one of err and file will be non-nil.
type partFile struct {
	// part is the part to which this partFile applies.
	part part
	// err will be non-nil if there was an error producing the file of the part
	// of the dump.
	err error
	// file will be non-nil in a successful result and will be the file that
	// will be included in the dump tarball.
	file *os.File
}

// processPart is called by worker goroutines to process a single part.
func processPart(ctx context.Context, part part) partFile {
	tmpfile, err := ioutil.TempFile("", "dump")
	if err != nil {
		return partFile{
			part: part,
			err:  fmt.Errorf("error creating temp file: %v", err),
		}
	}
	if err := os.Remove(tmpfile.Name()); err != nil {
		log.Printf("dump: error removing temp file %s: %v", tmpfile.Name(), err)
	}
	if err := part.f(ctx, tmpfile); err != nil {
		_ = tmpfile.Close()
		return partFile{
			part: part,
			err:  fmt.Errorf("error writing part contents: %v", err),
		}
	}
	if _, err := tmpfile.Seek(0, 0); err != nil {
		_ = tmpfile.Close()
		return partFile{
			part: part,
			err:  fmt.Errorf("error seeking to read temp file for dump: %v", err),
		}
	}
	// The returned file will be closed downstream after its contents have been
	// written to the dump.
	return partFile{part: part, file: tmpfile}
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

// writePart writes a single part to tw. pfx is the path that will be prepended
// to the part name to construct the full path of the entry in the archive.
func writePart(pfx string, p partFile, tw *tar.Writer) (err error) {
	if p.err != nil {
		return fmt.Errorf("error dumping %s: %v", p.part.name, p.err)
	}
	defer func() {
		closeErr := p.file.Close()
		if err == nil && closeErr != nil {
			err = fmt.Errorf("error closing temp file %q: %v", p.file.Name(), closeErr)
		}
	}()
	if tarErr := writeTar(pfx+"/"+p.part.name, p.file, tw); tarErr != nil {
		return fmt.Errorf("error writing %s to archive: %v", p.part.name, tarErr)
	}
	return nil
}

// WriteDump writes the dump to w. pfx is prepended to the names of the parts of
// the dump, e.g. if pfx == "dump-123" and part name == "cpu", "dump-123/cpu"
// will be written into the archive. It returns no error, as it is best-effort.
func (reg *Registry) WriteDump(ctx context.Context, pfx string, w io.Writer) {
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
	partFileC := make(chan partFile, concurrency)
	go func() {
		defer close(partFileC)
		err := traverse.Parallel.Each(len(parts), func(i int) error {
			partCtx, partCtxCancel := context.WithTimeout(ctx, 2*time.Minute)
			partFile := processPart(partCtx, parts[i])
			partCtxCancel()
			partFileC <- partFile
			return nil
		})
		if err != nil {
			log.Error.Printf("dump: error processing parts: %v", err)
			return
		}
	}()
	for p := range partFileC {
		if err := writePart(pfx, p, tw); err != nil {
			log.Error.Printf("dump: error processing part %s: %v", p.part.name, err)
		}
	}
}

var createTimeFormat = "2006-01-02-1504"

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	return fmt.Sprintf("%02dh%02dm%02ds", h, m, s)
}

// ServeHTTP serves the dump as a tarball, with a Content-Disposition set with
// a unique filename.
func (reg *Registry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/gzip")
	pfx := Name()
	filename := pfx + ".tar.gz"
	w.Header().Set("Content-Disposition", "attachment; filename="+filename)
	reg.WriteDump(r.Context(), pfx, w)
}
