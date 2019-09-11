// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package file

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
)

type localImpl struct{}

type accessMode int

const (
	readonly      accessMode = iota // file opened by Open.
	writeonlyFile                   // regular file opened by Create.
	writeonlyDev                    // device or socket opened by Create.
)

type localInfo struct {
	size    int64
	modTime time.Time
}

type localFile struct {
	f        *os.File
	mode     accessMode
	path     string // User-supplied path.
	realPath string // Path after symlink resolution.
}

type localLister struct {
	prefix  string
	err     error
	path    string
	info    os.FileInfo
	todo    []string
	recurse bool
}

func (impl *localImpl) String() string {
	return "local"
}

// Open implements file.Implementation.
func (impl *localImpl) Open(ctx context.Context, path string, _ ...Opts) (File, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = errors.E(err, errors.NotExist)
		}
		return nil, err
	}
	return &localFile{f: f, mode: readonly, path: path}, nil
}

// Create implements file.Implementation.  To make writes appear linearizable,
// it creates a temporary file with name <path>.tmp, then renames the temp file
// to <path> on Close.
func (*localImpl) Create(ctx context.Context, path string, _ ...Opts) (File, error) {
	if path == "" { // Detect common errors quickly.
		return nil, fmt.Errorf("file.Create: empty pathname")
	}
	realPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		// This happens when the file doesn't exist, including the case where path
		// is a symlink and the symlink destination doesn't exist.
		//
		// TODO(saito) UNIX open(2), O_CREAT creates the symlink destination in this
		// case.  Instead, here we create a tempfile in Dir(path), then delete the
		// symlink on close.
		realPath = path
	}
	if stat, err := os.Stat(path); err == nil && ((stat.Mode()&os.ModeDevice != 0) || (stat.Mode()&os.ModeNamedPipe != 0) || (stat.Mode()&os.ModeSocket != 0)) {
		f, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		return &localFile{f: f, mode: writeonlyDev, path: path, realPath: realPath}, nil
	}

	// filepath.Dir just strips the last "/" if path ends with "/". Else, it
	// removes the last component of the path. That's what we want.
	dir := filepath.Dir(realPath)
	f, err := ioutil.TempFile(dir, filepath.Base(realPath)+".tmp")
	if err != nil {
		if err = os.MkdirAll(dir, 0777); err != nil {
			log.Error.Printf("mkdir %v: error %v ", dir, err)
		}
		f, err = ioutil.TempFile(dir, "localtmp")
		if err != nil {
			return nil, err
		}
	}
	return &localFile{f: f, mode: writeonlyFile, path: path, realPath: realPath}, nil
}

// Close implements file.Implementation.
func (f *localFile) Close(ctx context.Context) error {
	return f.close(ctx, true)
}

// CloseNoSync closes the file without an fsync.
func (f *localFile) CloseNoSync(ctx context.Context) error {
	return f.close(ctx, false)
}

func (f *localFile) close(_ context.Context, doSync bool) error {
	switch f.mode {
	case readonly, writeonlyDev:
		return f.f.Close()
	default:
		var err error
		if doSync {
			err = f.f.Sync()
		}
		if e := f.f.Close(); e != nil && err == nil {
			err = e
		}
		if err != nil {
			_ = os.Remove(f.f.Name())
			return err
		}
		return os.Rename(f.f.Name(), f.realPath)
	}
}

// Discard implements file.File.
func (f *localFile) Discard(ctx context.Context) {
	switch f.mode {
	case readonly, writeonlyDev:
		return
	}
	if err := f.f.Close(); err != nil {
		log.Printf("discard %s: close: %v", f.Name(), err)
	}
	if err := os.Remove(f.f.Name()); err != nil {
		log.Printf("discard %s: remove: %v", f.Name(), err)
	}
}

// String implements file.File.
func (f *localFile) String() string {
	return f.path
}

// Name implements file.File.
func (f *localFile) Name() string {
	return f.path
}

// Reader implements file.File
func (f *localFile) Reader(context.Context) io.ReadSeeker {
	if f.mode != readonly {
		return NewError(fmt.Errorf("reader %v: file is not opened in read mode", f.Name()))
	}
	return f.f
}

// Writer implements file.Writer
func (f *localFile) Writer(context.Context) io.Writer {
	if f.mode == readonly {
		return NewError(fmt.Errorf("writer %v: file is not opened in write mode", f.Name()))
	}
	return f.f
}

// List implements file.Implementation
func (impl *localImpl) List(ctx context.Context, prefix string, recurse bool) Lister {
	return &localLister{prefix: prefix, todo: []string{prefix}, recurse: recurse}
}

// Remove implements file.Implementation.
func (*localImpl) Remove(ctx context.Context, path string) error {
	return os.Remove(path)
}

func (*localImpl) Presign(_ context.Context, path, _ string, _ time.Duration) (string, error) {
	return "", errors.E(errors.NotSupported,
		fmt.Sprintf("presign %v: local files not supported", path))
}

// Stat implements file.Implementation
func (impl *localImpl) Stat(ctx context.Context, path string, _ ...Opts) (Info, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = errors.E(err, errors.NotExist)
		}
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("stat %v: is a directory", path)
	}
	return &localInfo{size: info.Size(), modTime: info.ModTime()}, nil
}

// Stat implements file.File
func (f *localFile) Stat(context.Context) (Info, error) {
	info, err := f.f.Stat()
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("stat %v: is a directory", f.path)
	}
	return &localInfo{size: info.Size(), modTime: info.ModTime()}, nil
}

func (i *localInfo) Size() int64        { return i.size }
func (i *localInfo) ModTime() time.Time { return i.modTime }

// Scan implements Lister.Scan.
func (l *localLister) Scan() bool {

	for {
		if len(l.todo) == 0 || l.err != nil {
			return false
		}
		l.path, l.todo = l.todo[0], l.todo[1:]
		l.info, l.err = os.Stat(l.path)
		if os.IsNotExist(l.err) {
			l.err = nil
			continue
		}
		if l.err != nil {
			return false
		}
		if !l.info.IsDir() {
			return true
		}
		if l.recurse || l.path == l.prefix {
			var paths []string
			paths, l.err = readDirNames(l.path)
			if l.err != nil {
				return false
			}
			for i := range paths {
				paths[i] = filepath.Join(l.path, paths[i])
			}
			l.todo = append(paths, l.todo...)
		}
		if l.showDirs() && l.path != l.prefix {
			return true
		}
		continue
	}
}

// Path returns the most recent path that was scanned.
func (l *localLister) Path() string {
	return l.path
}

// Info returns the os.FileInfo for the most recent path scanned, or nil if IsDir() is true
func (l *localLister) Info() Info {
	infoSize := l.info.Size()
	if l.info.IsDir() {
		return nil
	}
	return &localInfo{size: infoSize, modTime: l.info.ModTime()}
}

// Info returns the os.FileInfo for the most recent path scanned.
func (l *localLister) IsDir() bool {
	return l.info.IsDir()
}

// Err returns the first error that occurred while scanning.
func (l *localLister) Err() error {
	return l.err
}

// showDirs controls whether directories are returned during a scan
func (l *localLister) showDirs() bool {
	return !l.recurse
}

// readDirNames reads the directory named by dirname and returns
// a sorted list of directory entries.
func readDirNames(dirname string) ([]string, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	names, err := f.Readdirnames(-1)
	if e := f.Close(); e != nil && err == nil {
		err = e
	}
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

// NewLocalImplementation returns a new file.Implementation for the local file system
// that uses Go's native "os" module. This function is only for unittests.
// Applications should use functions such as file.Open, file.Create to access
// the local file system.
func NewLocalImplementation() Implementation { return &localImpl{} }
