package fsnodefuse

import (
	"context"
	"fmt"
	"io"
	"syscall"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/file/fsnodefuse/trailingbuf"
	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/base/ioctx/fsctx"
	"github.com/grailbio/base/ioctx/spliceio"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/sync/loadingcache"
	"github.com/grailbio/base/sync/loadingcache/ctxloadingcache"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// makeHandle makes a fs.FileHandle for the given file, constructing an
// appropriate implementation given the flags and file implementation.
func makeHandle(n *regInode, flags uint32, file fsctx.File) (fs.FileHandle, error) {
	var (
		spliceioReaderAt, isSpliceioReaderAt = file.(spliceio.ReaderAt)
		ioctxReaderAt, isIoctxReaderAt       = file.(ioctx.ReaderAt)
	)
	if (flags&fuse.O_ANYWRITE) == 0 && !isSpliceioReaderAt && !isIoctxReaderAt {
		tbReaderAt := trailingbuf.New(file, maxReadAhead)
		return sizingHandle{
			n:     n,
			f:     file,
			r:     tbReaderAt,
			cache: &n.cache,
		}, nil
	}
	var r fs.FileReader
	switch {
	case isSpliceioReaderAt:
		r = fileReaderSpliceio{spliceioReaderAt}
	case isIoctxReaderAt:
		r = fileReaderIoctx{ioctxReaderAt}
	case (flags & syscall.O_WRONLY) != syscall.O_WRONLY:
		return nil, errors.E(
			errors.NotSupported,
			fmt.Sprintf("%T must implement spliceio.SpliceReaderAt or ioctx.ReaderAt", file),
		)
	}
	w, _ := file.(Writable)
	return &handle{
		f:     file,
		r:     r,
		w:     w,
		cache: &n.cache,
	}, nil
}

// sizingHandle infers the size of the underlying fsctx.File stream based on
// EOF.
type sizingHandle struct {
	n     *regInode
	f     fsctx.File
	r     *trailingbuf.ReaderAt
	cache *loadingcache.Map
}

var (
	_ fs.FileGetattrer = (*sizingHandle)(nil)
	_ fs.FileReader    = (*sizingHandle)(nil)
	_ fs.FileReleaser  = (*sizingHandle)(nil)
)

func (h sizingHandle) Getattr(ctx context.Context, a *fuse.AttrOut) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, h.cache)

	// Note: Implementations that don't know the exact data size in advance may used some fixed
	// overestimate for size.
	statInfo, err := h.f.Stat(ctx)
	if err != nil {
		return errToErrno(err)
	}
	info := fsnode.CopyFileInfo(statInfo)

	localSize, localKnown, err := h.r.Size(ctx)
	if err != nil {
		return errToErrno(err)
	}

	h.n.defaultSizeMu.RLock()
	sharedKnown := h.n.defaultSizeKnown
	sharedSize := h.n.defaultSize
	h.n.defaultSizeMu.RUnlock()

	if localKnown && !sharedKnown {
		// This may be the first handle to reach EOF. Update the shared data.
		h.n.defaultSizeMu.Lock()
		if !h.n.defaultSizeKnown {
			h.n.defaultSizeKnown = true
			h.n.defaultSize = localSize
			sharedSize = localSize
		} else {
			sharedSize = h.n.defaultSize
		}
		h.n.defaultSizeMu.Unlock()
		sharedKnown = true
	}
	if sharedKnown {
		if localKnown && localSize != sharedSize {
			log.Error.Printf(
				"fsnodefuse.defaultHandle.Getattr: size-at-EOF mismatch: this handle: %d, earlier: %d",
				localSize, sharedSize)
			return syscall.EIO
		}
		info = info.WithSize(sharedSize)
	}
	setAttrFromFileInfo(&a.Attr, info)
	return fs.OK
}

func (h sizingHandle) Read(ctx context.Context, dst []byte, off int64) (_ fuse.ReadResult, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, h.cache)

	n, err := h.r.ReadAt(ctx, dst, off)
	if err == io.EOF {
		err = nil
	}
	return fuse.ReadResultData(dst[:n]), errToErrno(err)
}

func (h *sizingHandle) Release(ctx context.Context) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if h.f == nil {
		return syscall.EBADF
	}
	ctx = ctxloadingcache.With(ctx, h.cache)

	err := h.f.Close(ctx)
	h.f = nil
	h.r = nil
	h.cache = nil
	return errToErrno(err)
}

type fileReaderSpliceio struct{ spliceio.ReaderAt }

func (r fileReaderSpliceio) Read(
	ctx context.Context,
	dest []byte,
	off int64,
) (_ fuse.ReadResult, errno syscall.Errno) {
	fd, fdSize, fdOff, err := r.SpliceReadAt(ctx, len(dest), off)
	if err != nil {
		return nil, errToErrno(err)
	}
	return fuse.ReadResultFd(fd, fdOff, fdSize), fs.OK
}

type fileReaderIoctx struct{ ioctx.ReaderAt }

func (r fileReaderIoctx) Read(
	ctx context.Context,
	dest []byte,
	off int64,
) (_ fuse.ReadResult, errno syscall.Errno) {
	n, err := r.ReadAt(ctx, dest, off)
	if err == io.EOF {
		err = nil
	}
	return fuse.ReadResultData(dest[:n]), errToErrno(err)
}

type (
	// Writable is the interface that must be implemented by files returned by
	// (fsnode.Leaf).OpenFile to support writing.
	Writable interface {
		WriteAt(ctx context.Context, p []byte, off int64) (n int, err error)
		Truncate(ctx context.Context, n int64) error
		Flush(ctx context.Context) error
		Fsync(ctx context.Context) error
	}
	// handle is an implementation of fs.FileHandle that wraps an fsctx.File.
	// The behavior of the handle depends on the functions implemented by the
	// fsctx.File value.
	handle struct {
		f     fsctx.File
		r     fs.FileReader
		w     Writable
		cache *loadingcache.Map
	}
)

var (
	_ fs.FileFlusher   = (*handle)(nil)
	_ fs.FileFsyncer   = (*handle)(nil)
	_ fs.FileGetattrer = (*handle)(nil)
	_ fs.FileReader    = (*handle)(nil)
	_ fs.FileReleaser  = (*handle)(nil)
	_ fs.FileSetattrer = (*handle)(nil)
	_ fs.FileWriter    = (*handle)(nil)
)

func (h handle) Getattr(ctx context.Context, out *fuse.AttrOut) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if h.f == nil {
		return syscall.EBADF
	}
	ctx = ctxloadingcache.With(ctx, h.cache)
	info, err := h.f.Stat(ctx)
	if err != nil {
		return errToErrno(err)
	}
	if statT := fuse.ToStatT(info); statT != nil {
		// Stat returned a *syscall.Stat_t, so just plumb that through.
		out.FromStat(statT)
	} else {
		setAttrFromFileInfo(&out.Attr, info)
	}
	out.SetTimeout(getCacheTimeout(h.f))
	return fs.OK
}

func (h handle) Setattr(
	ctx context.Context,
	in *fuse.SetAttrIn,
	out *fuse.AttrOut,
) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if h.f == nil {
		return syscall.EBADF
	}
	if h.w == nil {
		return syscall.ENOSYS
	}
	h.cache.DeleteAll()
	if usize, ok := in.GetSize(); ok {
		return errToErrno(h.w.Truncate(ctx, int64(usize)))
	}
	return fs.OK
}

func (h handle) Read(
	ctx context.Context,
	dst []byte,
	off int64,
) (_ fuse.ReadResult, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if h.f == nil {
		return nil, syscall.EBADF
	}
	if h.r == nil {
		return nil, syscall.ENOSYS
	}
	ctx = ctxloadingcache.With(ctx, h.cache)
	return h.r.Read(ctx, dst, off)
}

func (h handle) Write(
	ctx context.Context,
	p []byte,
	off int64,
) (_ uint32, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if h.f == nil {
		return 0, syscall.EBADF
	}
	if h.w == nil {
		return 0, syscall.ENOSYS
	}
	ctx = ctxloadingcache.With(ctx, h.cache)
	n, err := h.w.WriteAt(ctx, p, off)
	return uint32(n), errToErrno(err)
}

func (h handle) Flush(ctx context.Context) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if h.f == nil {
		return syscall.EBADF
	}
	if h.w == nil {
		return fs.OK
	}
	ctx = ctxloadingcache.With(ctx, h.cache)
	err := h.w.Flush(ctx)
	return errToErrno(err)
}

func (h handle) Fsync(ctx context.Context, flags uint32) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if h.f == nil {
		return syscall.EBADF
	}
	if h.w == nil {
		return fs.OK
	}
	ctx = ctxloadingcache.With(ctx, h.cache)
	err := h.w.Fsync(ctx)
	return errToErrno(err)
}

func (h *handle) Release(ctx context.Context) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if h.f == nil {
		return syscall.EBADF
	}
	ctx = ctxloadingcache.With(ctx, h.cache)
	err := h.f.Close(ctx)
	h.f = nil
	h.r = nil
	h.w = nil
	h.cache = nil
	return errToErrno(err)
}
