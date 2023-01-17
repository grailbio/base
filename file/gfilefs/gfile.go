// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package gfilefs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/fsnodefuse"
	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/base/ioctx/fsctx"
	"github.com/grailbio/base/sync/ctxsync"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// gfile implements fsctx.File and fsnodefuse.Writable to represent open
// gfilefs files.
type gfile struct {
	// n is the node for which this instance is an open file.
	n *fileNode
	// flag holds the flag bits specified when this file was opened.
	flag int

	// readerAt is an optional ReaderAt implementation. It may only be set upon
	// construction, and must not be modified later. Thus, it can be read by
	// multiple goroutines without holding the lock, without a data race.
	// When non-nil, it serves ReadAt requests concurrently, without ops.
	// Otherwise, gfile.ReadAt uses ops.ReadAt.
	readerAt ioctx.ReaderAt

	// mu provides mutually exclusive access to the fields below.
	mu ctxsync.Mutex
	// requestedSize is the size requested by Truncate.  Note that we only
	// really support truncation to 0 well, as it is mainly used by go-fuse for
	// truncation when handling O_TRUNC.
	requestedSize int64
	// flushed is true if there are no writes that need to be flushed.  If
	// flushed == true, Flush is a no-op.
	flushed bool
	// anyWritten tracks whether we have written any bytes to this file.  We
	// use this to decide whether we can use direct writing.
	anyWritten bool
	// ops handles underlying I/O operations.  See ioOps.  ops may be lazily
	// populated, and it may be reassigned over the lifetime of the file, e.g.
	// after truncation, we may switch to an ops that no longer uses a
	// temporary file.
	ops ioOps
}

var (
	_ fsctx.File          = (*gfile)(nil)
	_ ioctx.ReaderAt      = (*gfile)(nil)
	_ fsnodefuse.Writable = (*gfile)(nil)
)

// OpenFile opens the file at n and returns a *gfile representing it for file
// operations.
func OpenFile(ctx context.Context, n *fileNode, flag int) (*gfile, error) {
	gf := &gfile{
		n:             n,
		flag:          flag,
		requestedSize: -1,
		// Creation and truncation require flushing.
		flushed: (flag&os.O_CREATE) == 0 && (flag&os.O_TRUNC) == 0,
	}
	if (flag & int(fuse.O_ANYWRITE)) == 0 {
		// Read-only files are initialized eagerly, as it is cheap, and we can
		// immediately return any errors.  Writable files are initialized
		// lazily; see lockedInitOps.
		f, err := file.Open(ctx, n.path)
		if err != nil {
			return nil, err
		}
		gf.ops = &directRead{
			f: f,
			r: f.Reader(context.Background()), // TODO: Tie to gf lifetime?
		}
		gf.readerAt = gf.ops
		return gf, nil
	}
	return gf, nil
}

// Stat implements fsctx.File.
func (gf *gfile) Stat(ctx context.Context) (os.FileInfo, error) {
	if err := gf.mu.Lock(ctx); err != nil {
		return nil, err
	}
	defer gf.mu.Unlock()
	if err := gf.lockedInitOps(ctx); err != nil {
		return nil, err
	}
	info, err := gf.ops.Stat(ctx)
	if err != nil {
		if errors.Recover(err).Kind == errors.NotSupported {
			return gf.n.Info(), nil
		}
		return nil, errors.E(err, "getting stat info from underlying I/O")
	}
	newInfo := gf.n.fsnodeInfo().
		WithModTime(info.ModTime()).
		WithSize(info.Size())
	gf.n.setFsnodeInfo(newInfo)
	return newInfo, nil
}

// Read implements fsctx.File.
func (gf *gfile) Read(ctx context.Context, p []byte) (int, error) {
	if err := gf.mu.Lock(ctx); err != nil {
		return 0, err
	}
	defer gf.mu.Unlock()
	if err := gf.lockedInitOps(ctx); err != nil {
		return 0, err
	}
	return gf.ops.Read(ctx, p)
}

// ReadAt implements ioctx.ReaderAt.
func (gf *gfile) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if gf.readerAt != nil {
		return gf.readerAt.ReadAt(ctx, p, off)
	}
	if err := gf.mu.Lock(ctx); err != nil {
		return 0, err
	}
	defer gf.mu.Unlock()
	if err := gf.lockedInitOps(ctx); err != nil {
		return 0, err
	}
	return gf.ops.ReadAt(ctx, p, off)
}

// WriteAt implements fsnodefuse.Writable.
func (gf *gfile) WriteAt(ctx context.Context, p []byte, off int64) (int, error) {
	if err := gf.mu.Lock(ctx); err != nil {
		return 0, err
	}
	defer gf.mu.Unlock()
	if err := gf.lockedInitOps(ctx); err != nil {
		return 0, err
	}
	n, err := gf.ops.WriteAt(ctx, p, off)
	if err != nil {
		return n, err
	}
	gf.anyWritten = true
	gf.flushed = false
	return n, err
}

// Truncate implements fsnodefuse.Writable.
func (gf *gfile) Truncate(ctx context.Context, size int64) error {
	if err := gf.mu.Lock(ctx); err != nil {
		return err
	}
	defer gf.mu.Unlock()
	gf.flushed = false
	if gf.ops == nil {
		gf.requestedSize = 0
		return nil
	}
	return gf.ops.Truncate(ctx, size)
}

// Flush implements fsnodefuse.Writable.
func (gf *gfile) Flush(ctx context.Context) error {
	if err := gf.mu.Lock(ctx); err != nil {
		return err
	}
	defer gf.mu.Unlock()
	return gf.lockedFlush()
}

// Fsync implements fsnodefuse.Writable.
func (gf *gfile) Fsync(ctx context.Context) error {
	// We treat Fsync as Flush, mostly because leaving it unimplemented
	// (ENOSYS) breaks too many applications.
	return gf.Flush(ctx)
}

// Close implements fsctx.File.
func (gf *gfile) Close(ctx context.Context) error {
	if err := gf.mu.Lock(ctx); err != nil {
		return err
	}
	defer gf.mu.Unlock()
	if gf.ops == nil {
		return nil
	}
	return gf.ops.Close(ctx)
}

// lockedInitOps initializes the ops that handle the underlying I/O operations
// of gf.  This is done lazily in some cases, as it may be expensive, e.g.
// downloading a remotely stored file locally.  Initialization may also depend
// on other operations, e.g. if the first manipulation is truncation, then we
// won't download existing data.  gf.ops is non-nil iff lockedInitOps returns a
// nil error.  The caller must have locked access granted by gf.mutexCh.
func (gf *gfile) lockedInitOps(ctx context.Context) (err error) {
	if gf.ops != nil {
		return nil
	}
	// base/file does not expose an API to open a file for writing without
	// creating it, so writing implies creation.
	const tmpPattern = "gfilefs-"
	var (
		rdwr = (gf.flag & os.O_RDWR) == os.O_RDWR
		// Treat O_EXCL as O_TRUNC, as the file package does not support
		// O_EXCL.
		trunc = !gf.anyWritten &&
			(gf.requestedSize == 0 ||
				(gf.flag&os.O_TRUNC) == os.O_TRUNC ||
				(gf.flag&os.O_EXCL) == os.O_EXCL)
	)
	switch {
	case trunc && rdwr:
		tmp, err := ioutil.TempFile("", tmpPattern)
		if err != nil {
			return errors.E(err, "making temp file")
		}
		gf.ops = &tmpIO{n: gf.n, f: tmp}
		return nil
	case trunc:
		f, err := file.Create(ctx, gf.n.path)
		if err != nil {
			return errors.E(err, fmt.Sprintf("creating file at %q", gf.n.path))
		}
		// This is a workaround for the fact that directWrite ops do not
		// support Stat (as write-only s3files do not support Stat).  Callers,
		// e.g. fsnodefuse, may fall back to use the node's information, so we
		// zero that to keep a sensible view.
		gf.n.setFsnodeInfo(gf.n.fsnodeInfo().WithSize(0))
		gf.ops = &directWrite{
			n:   gf.n,
			f:   f,
			w:   f.Writer(context.Background()), // TODO: Tie to gf lifetime?
			off: 0,
		}
		return nil
	default:
		// existing reads out existing file contents.  Contents may be empty if
		// no file exists yet.
		var existing io.Reader
		f, err := file.Open(ctx, gf.n.path)
		if err == nil {
			existing = f.Reader(ctx)
		} else {
			if errors.Is(errors.NotExist, err) {
				if !rdwr {
					// Write-only and no existing file, so we can use direct
					// I/O.
					f, err = file.Create(ctx, gf.n.path)
					if err != nil {
						return errors.E(err, fmt.Sprintf("creating file at %q", gf.n.path))
					}
					gf.ops = &directWrite{
						n:   gf.n,
						f:   f,
						w:   f.Writer(context.Background()), // TODO: Tie to gf lifetime?
						off: 0,
					}
					return nil
				}
				// No existing file, so there are no existing contents.
				err = nil
				existing = &bytes.Buffer{}
			} else {
				return errors.E(err, fmt.Sprintf("opening file for %q", gf.n.path))
			}
		}
		tmp, err := ioutil.TempFile("", tmpPattern)
		if err != nil {
			// fp was opened for reading, so don't worry about the error on
			// Close.
			_ = f.Close(ctx)
			return errors.E(err, "making temp file")
		}
		_, err = io.Copy(tmp, existing)
		if err != nil {
			// We're going to report the copy error, so we treat closing as
			// best-effort.
			_ = f.Close(ctx)
			_ = tmp.Close()
			return errors.E(err, fmt.Sprintf("copying current contents to temp file %q", tmp.Name()))
		}
		gf.ops = &tmpIO{n: gf.n, f: tmp}
		return nil
	}
}

// lockedFlush flushes writes to the backing write I/O state.  The caller must
// have locked access granted by gf.mutexCh.
func (gf *gfile) lockedFlush() (err error) {
	// We use a background context when flushing as a workaround for handling
	// interrupted operations, particularly from Go clients.  As of Go 1.14,
	// slow system calls may see more EINTR errors[1].  While most file
	// operations are automatically retried[2], closing (which results in
	// flushing) is not[3].  Ultimately, clients may see spurious, confusing
	// failures calling (*os.File).Close.  Given that it is extremely uncommon
	// for callers to retry, we ignore interrupts to avoid the confusion.  The
	// significant downside is that intentional interruption, e.g. CTRL-C on a
	// program that is taking too long, is also ignored, so processes can
	// appear hung.
	//
	// TODO: Consider a better way of handling this problem.
	//
	// [1] https://go.dev/doc/go1.14#runtime
	// [2] https://github.com/golang/go/commit/6b420169d798c7ebe733487b56ea5c3fa4aab5ce
	// [3] https://github.com/golang/go/blob/go1.17.8/src/internal/poll/fd_unix.go#L79-L83
	ctx := context.Background()
	if gf.flushed {
		return nil
	}
	defer func() {
		if err == nil {
			gf.flushed = true
		}
	}()
	if (gf.flag & int(fuse.O_ANYWRITE)) != 0 {
		if err = gf.lockedInitOps(ctx); err != nil {
			return err
		}
	}
	reuseOps, err := gf.ops.Flush(ctx)
	if err != nil {
		return err
	}
	if !reuseOps {
		gf.ops = nil
	}
	return nil
}

// ioOps handles the underlying I/O operations for a *gfile.  Implementations
// may directly call base/file or use a temporary file on local disk until
// flush.
type ioOps interface {
	Stat(ctx context.Context) (file.Info, error)
	Read(ctx context.Context, p []byte) (int, error)
	ReadAt(ctx context.Context, p []byte, off int64) (int, error)
	WriteAt(ctx context.Context, p []byte, off int64) (int, error)
	Truncate(ctx context.Context, size int64) error
	Flush(ctx context.Context) (reuseOps bool, _ error)
	Close(ctx context.Context) error
}

// directRead implements ioOps.  It reads directly using base/file and does not
// support writes, e.g. to handle O_RDONLY.
type directRead struct {
	f   file.File
	r   io.ReadSeeker
	off int64
}

var _ ioOps = (*directRead)(nil)

func (ops directRead) Stat(ctx context.Context) (file.Info, error) {
	return ops.f.Stat(ctx)
}

func (ops directRead) Read(ctx context.Context, p []byte) (int, error) {
	return ops.r.Read(p)
}

func (ops *directRead) ReadAt(ctx context.Context, p []byte, off int64) (_ int, err error) {
	rc := ops.f.OffsetReader(off)
	defer errors.CleanUpCtx(ctx, rc.Close, &err)
	n, err := io.ReadFull(ioctx.ToStdReader(ctx, rc), p)
	if err == io.ErrUnexpectedEOF {
		return n, io.EOF
	}
	return n, err
}

func (directRead) WriteAt(ctx context.Context, p []byte, off int64) (int, error) {
	return 0, errors.E(errors.Invalid, "writing read-only file")
}

func (directRead) Truncate(ctx context.Context, size int64) error {
	return errors.E(errors.Invalid, "cannot truncate read-only file")
}

func (directRead) Flush(ctx context.Context) (reuseOps bool, _ error) {
	return true, nil
}

func (ops directRead) Close(ctx context.Context) error {
	return ops.f.Close(ctx)
}

// directWrite implements ioOps.  It writes directly using base/file and does
// not support reads, e.g. to handle O_WRONLY|O_TRUNC.
type directWrite struct {
	n   *fileNode
	f   file.File
	w   io.Writer
	off int64
}

var _ ioOps = (*directWrite)(nil)

func (ops directWrite) Stat(ctx context.Context) (file.Info, error) {
	return ops.f.Stat(ctx)
}

func (directWrite) Read(ctx context.Context, p []byte) (int, error) {
	return 0, errors.E(errors.Invalid, "reading write-only file")
}

func (directWrite) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	return 0, errors.E(errors.Invalid, "reading write-only file")
}

func (ops *directWrite) WriteAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off != ops.off {
		return 0, errors.E(errors.NotSupported, "non-contiguous write")
	}
	n, err := ops.w.Write(p)
	ops.off += int64(n)
	return n, err
}

func (ops directWrite) Truncate(ctx context.Context, size int64) error {
	if ops.off != size {
		return errors.E(errors.NotSupported, "truncating to %d not supported by direct I/O")
	}
	return nil
}

func (ops *directWrite) Flush(ctx context.Context) (reuseOps bool, _ error) {
	err := ops.f.Close(ctx)
	ops.n.setFsnodeInfo(
		ops.n.fsnodeInfo().
			WithModTime(time.Now()).
			WithSize(ops.off),
	)
	// Clear to catch accidental reuse.
	*ops = directWrite{}
	return false, err
}

func (ops directWrite) Close(ctx context.Context) error {
	return ops.f.Close(ctx)
}

// tmpIO implements ioOps.  It is backed by a temporary local file, e.g. to
// handle O_RDWR.
type tmpIO struct {
	n *fileNode
	f *os.File // refers to a file in -tmp-dir.
}

var _ ioOps = (*tmpIO)(nil)

func (ops tmpIO) Stat(_ context.Context) (file.Info, error) {
	return ops.f.Stat()
}

func (ops tmpIO) Read(_ context.Context, p []byte) (int, error) {
	return ops.f.Read(p)
}

func (ops tmpIO) ReadAt(_ context.Context, p []byte, off int64) (int, error) {
	return ops.f.ReadAt(p, off)
}

func (ops tmpIO) WriteAt(_ context.Context, p []byte, off int64) (int, error) {
	return ops.f.WriteAt(p, off)
}

func (ops tmpIO) Truncate(_ context.Context, size int64) error {
	return ops.f.Truncate(size)
}

func (ops *tmpIO) Flush(ctx context.Context) (reuseOps bool, err error) {
	dst, err := file.Create(ctx, ops.n.path)
	if err != nil {
		return false, errors.E(err, fmt.Sprintf("creating file %q", ops.n.path))
	}
	defer file.CloseAndReport(ctx, dst, &err)
	n, err := io.Copy(dst.Writer(ctx), &readerAdapter{r: ops.f})
	if err != nil {
		return false, errors.E(
			err,
			fmt.Sprintf("copying from %q to %q", ops.f.Name(), ops.n.path),
		)
	}
	ops.n.setFsnodeInfo(
		ops.n.fsnodeInfo().
			WithModTime(time.Now()).
			WithSize(n),
	)
	return true, nil
}

// readerAdapter adapts an io.ReaderAt to be an io.Reader, calling ReadAt and
// maintaining the offset for the next Read.
type readerAdapter struct {
	r   io.ReaderAt
	off int64
}

func (a *readerAdapter) Read(p []byte) (int, error) {
	n, err := a.r.ReadAt(p, a.off)
	a.off += int64(n)
	return n, err
}

func (ops *tmpIO) Close(_ context.Context) error {
	err := ops.f.Close()
	if errRemove := os.Remove(ops.f.Name()); errRemove != nil && err == nil {
		err = errors.E(errRemove, "removing tmpIO file")
	}
	return err
}
