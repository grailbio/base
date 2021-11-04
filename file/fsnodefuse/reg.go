package fsnodefuse

import (
	"context"
	"io"
	"sync"
	"syscall"

	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/base/ioctx/fsctx"
	"github.com/grailbio/base/ioctx/spliceio"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/sync/loadingcache"
	"github.com/grailbio/base/sync/loadingcache/ctxloadingcache"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type regInode struct {
	fs.Inode
	dirStreamUsageImpl
	cache loadingcache.Map

	mu sync.Mutex
	n  fsnode.Leaf
}

var (
	_ inodeEmbedder = (*regInode)(nil)

	_ fs.NodeOpener    = (*regInode)(nil)
	_ fs.NodeGetattrer = (*regInode)(nil)
	_ fs.NodeSetattrer = (*regInode)(nil)
)

func (n *regInode) Open(ctx context.Context, inFlags uint32) (_ fs.FileHandle, outFlags uint32, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, &n.cache)

	file, err := n.n.Open(ctx)
	if err != nil {
		return nil, 0, errToErrno(err)
	}
	if f, ok := file.(spliceio.ReaderAt); ok {
		return &spliceHandle{file, f, &n.cache}, 0, fs.OK
	}
	if f, ok := file.(ioctx.ReaderAt); ok {
		return &readerAtHandle{file, f, &n.cache}, 0, fs.OK
	}
	return &defaultHandle{f: file, cache: &n.cache}, 0, fs.OK
}

func (n *regInode) Getattr(ctx context.Context, h fs.FileHandle, a *fuse.AttrOut) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, &n.cache)

	if h != nil {
		if hg, ok := h.(fs.FileGetattrer); ok {
			return hg.Getattr(ctx, a)
		}
	}

	setAttrFromFileInfo(&a.Attr, n.n)
	a.SetTimeout(getCacheTimeout(n.n))
	return fs.OK
}

func (n *regInode) Setattr(ctx context.Context, _ fs.FileHandle, _ *fuse.SetAttrIn, a *fuse.AttrOut) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	n.cache.DeleteAll()
	if errno := n.NotifyContent(0 /* offset */, 0 /* len, zero means all */); errno != fs.OK {
		log.Error.Printf("regInode.Setattr %s: error from NotifyContent: %v", n.Path(nil), errno)
		return errToErrno(errno)
	}
	// TODO(josh): Is this the right invalidation, and does it work? Maybe page cache only matters
	// if we set some other flags in open or read to enable it?
	setAttrFromFileInfo(&a.Attr, n.n)
	a.SetTimeout(getCacheTimeout(n.n))
	return fs.OK
}

type spliceHandle struct {
	f     fsctx.File
	r     spliceio.ReaderAt
	cache *loadingcache.Map
}

func (h *spliceHandle) Read(ctx context.Context, dst []byte, off int64) (_ fuse.ReadResult, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	fd, fdSize, fdOff, err := h.r.SpliceReadAt(ctx, len(dst), off)
	if err != nil {
		return nil, errToErrno(err)
	}
	return fuse.ReadResultFd(fd, fdOff, fdSize), fs.OK
}

func (h *spliceHandle) Getattr(ctx context.Context, a *fuse.AttrOut) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, h.cache)

	info, err := h.f.Stat(ctx)
	if err != nil {
		return errToErrno(err)
	}
	a.FromStat(fuse.ToStatT(info))
	a.SetTimeout(getCacheTimeout(h.f))
	return fs.OK
}

func (h *spliceHandle) Release(ctx context.Context) (errno syscall.Errno) {
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

type readerAtHandle struct {
	f     fsctx.File
	r     ioctx.ReaderAt
	cache *loadingcache.Map
}

func (h *readerAtHandle) Read(ctx context.Context, dst []byte, off int64) (_ fuse.ReadResult, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, h.cache)

	var (
		rest = dst
		err  error
	)
	for len(rest) > 0 && err == nil {
		var n int
		n, err = h.r.ReadAt(ctx, rest, off)
		rest = rest[n:]
	}
	if err == io.EOF {
		// TODO: Maybe this handle should record EOF position and subsequently report that as file
		// size from Getattr, as defaultHandle does.
		err = nil
	}
	// TODO: Consider setting block size? If it affects client read chunk sizes.
	return fuse.ReadResultData(dst[:len(dst)-len(rest)]), errToErrno(err)
}

func (h *readerAtHandle) Getattr(ctx context.Context, a *fuse.AttrOut) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, h.cache)

	info, err := h.f.Stat(ctx)
	if err != nil {
		return errToErrno(err)
	}
	setAttrFromFileInfo(&a.Attr, info)
	a.SetTimeout(getCacheTimeout(h.f))
	return fs.OK
}

func (h *readerAtHandle) Release(ctx context.Context) (errno syscall.Errno) {
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

type defaultHandle struct {
	f     fsctx.File
	pos   int64
	eof   bool
	cache *loadingcache.Map
}

func (h *defaultHandle) Read(ctx context.Context, dst []byte, off int64) (_ fuse.ReadResult, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, h.cache)

	if h.eof {
		for i := range dst {
			dst[i] = 0
		}
		return fuse.ReadResultData(dst), 0
	}
	if off != h.pos {
		log.Debug.Printf("fsnodefuse.default.Read: refusing seek: pos: %d, off: %d", h.pos, off)
		return nil, syscall.EINVAL
	}

	var (
		nTotal int
		err    error
	)
	for buf := dst; len(buf) > 0 && err == nil; {
		var n int
		n, err = h.f.Read(ctx, buf)
		nTotal += n
		buf = buf[n:]
	}
	h.pos += int64(nTotal)
	if err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, errToErrno(err)
		}
		h.eof = true
	}
	return fuse.ReadResultData(dst[:nTotal]), 0
}

func (h *defaultHandle) Getattr(ctx context.Context, a *fuse.AttrOut) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, h.cache)

	info, err := h.f.Stat(ctx)
	if err != nil {
		return errToErrno(err)
	}
	setAttrFromFileInfo(&a.Attr, info)
	if h.eof {
		a.Size = uint64(h.pos)
	}
	// TODO: Can we set cache timeouts since we need to change size at EOF?
	// a.SetTimeout(getCacheTimeout(h.f))
	return fs.OK
}

func (h *defaultHandle) Release(ctx context.Context) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if h.f == nil {
		return syscall.EBADF
	}
	ctx = ctxloadingcache.With(ctx, h.cache)

	err := h.f.Close(ctx)
	h.f = nil
	h.cache = nil
	return errToErrno(err)
}
