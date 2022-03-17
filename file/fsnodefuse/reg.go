package fsnodefuse

import (
	"context"
	"io"
	"sync"
	"syscall"

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

// TODO: Fix BXDS-1029. Without this, readers of non-constant files may see staleness and
// concurrent readers of such files may see corruption.
type regInode struct {
	fs.Inode
	cache loadingcache.Map

	mu sync.Mutex
	n  fsnode.Leaf

	// defaultSize is a shared record of file size for all file handles of type defaultHandle
	// created for this inode. The first defaultHandle to reach EOF (for its io.Reader) sets
	// defaultSizeKnown and defaultSize and after that all other handles will return the same size
	// from Getattr calls.
	//
	// defaultHandle returns incorrect size information until the underlying Reader reaches EOF. The
	// kernel issues concurrent reads to prepopulate the page cache, for performance, and also
	// interleaves Getattr calls to confirm where EOF really is. Complicating matters, multiple open
	// handles share the page cache, allowing situations where one handle has populated the page
	// cache, reached EOF, and knows the right size, whereas another handle's Reader is not there
	// yet so it continues to use the fake size (which we may choose to be some giant number so
	// users keep going until the end). This seems to cause bugs where user programs think they got
	// real data past EOF (which is probably just padded/zeros).
	//
	// To avoid this problem, all open defaultHandles share a size value, after first EOF.
	// TODO: Document more loudly the requirement that fsnode.Leaf.Open's files must return
	// identical data (same size, same bytes) to avoid corrupt page cache interactions.
	//
	// TODO: Investigate more thoroughly or at least with a newer kernel (this was observed on
	// 4.15.0-1099-aws).
	defaultSizeMu    sync.RWMutex
	defaultSizeKnown bool
	defaultSize      int64
}

var (
	_ fs.InodeEmbedder = (*regInode)(nil)

	_ fs.NodeOpener    = (*regInode)(nil)
	_ fs.NodeGetattrer = (*regInode)(nil)
	_ fs.NodeSetattrer = (*regInode)(nil)
)

// maxReadAhead configures the kernel's maximum readahead for file handles on this FUSE mount
// (via ConfigureMount) and our corresponding "trailing" buffer.
//
// Our defaultHandle implements Read operations for fsctx.File objects that don't support random
// access or seeking. Generally this requires that the user reading such a file does so in-order.
// However, the kernel attempts to optimize i/o speed by reading ahead into the page cache and to
// do so it can issue concurrent reads for a few blocks ahead of the user's current position.
// We respond to such requests from our trailing buffer.
// TODO: Choose a value more carefully. This value was chosen fairly roughly based on some
// articles/discussion that suggested this was a kernel default.
const maxReadAhead = 512 * 1024

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
	readerAt := trailingbuf.New(file, maxReadAhead)
	return defaultHandle{n, &readerAtHandle{file, readerAt, &n.cache}, readerAt}, 0, fs.OK
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

	n, err := h.r.ReadAt(ctx, dst, off)
	if err == io.EOF {
		err = nil
	}
	return fuse.ReadResultData(dst[:n]), errToErrno(err)
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

// defaultHandle is similar to readerAtHandle but also infers the size of the underlying stream
// based on EOF.
type defaultHandle struct {
	n *regInode
	*readerAtHandle
	r *trailingbuf.ReaderAt
}

func (h defaultHandle) Getattr(ctx context.Context, a *fuse.AttrOut) (errno syscall.Errno) {
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
