package fsnodefuse

import (
	"context"
	"os"
	"sync"
	"syscall"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/fsnode"
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

	// defaultSize is a shared record of file size for all file handles of type sizingHandle
	// created for this inode. The first sizingHandle to reach EOF (for its io.Reader) sets
	// defaultSizeKnown and defaultSize and after that all other handles will return the same size
	// from Getattr calls.
	//
	// sizingHandle returns incorrect size information until the underlying Reader reaches EOF. The
	// kernel issues concurrent reads to prepopulate the page cache, for performance, and also
	// interleaves Getattr calls to confirm where EOF really is. Complicating matters, multiple open
	// handles share the page cache, allowing situations where one handle has populated the page
	// cache, reached EOF, and knows the right size, whereas another handle's Reader is not there
	// yet so it continues to use the fake size (which we may choose to be some giant number so
	// users keep going until the end). This seems to cause bugs where user programs think they got
	// real data past EOF (which is probably just padded/zeros).
	//
	// To avoid this problem, all open sizingHandles share a size value, after first EOF.
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
// Our sizingHandle implements Read operations for read-only fsctx.File objects that don't support
// random access or seeking. Generally this requires that the user reading such a file does so
// in-order. However, the kernel attempts to optimize i/o speed by reading ahead into the page cache
// and to do so it can issue concurrent reads for a few blocks ahead of the user's current position.
// We respond to such requests from our trailing buffer.
// TODO: Choose a value more carefully. This value was chosen fairly roughly based on some
// articles/discussion that suggested this was a kernel default.
const maxReadAhead = 512 * 1024

func (n *regInode) Open(ctx context.Context, inFlags uint32) (_ fs.FileHandle, outFlags uint32, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, &n.cache)
	file, err := n.n.OpenFile(ctx, int(inFlags))
	if err != nil {
		return nil, 0, errToErrno(err)
	}
	h, err := makeHandle(n, inFlags, file)
	return h, 0, errToErrno(err)
}

func (n *regInode) Getattr(ctx context.Context, h fs.FileHandle, a *fuse.AttrOut) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, &n.cache)

	if h != nil {
		if hg, ok := h.(fs.FileGetattrer); ok {
			return hg.Getattr(ctx, a)
		}
	}

	setAttrFromFileInfo(&a.Attr, n.n.Info())
	a.SetTimeout(getCacheTimeout(n.n))
	return fs.OK
}

func (n *regInode) Setattr(ctx context.Context, h fs.FileHandle, in *fuse.SetAttrIn, a *fuse.AttrOut) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if h, ok := h.(fs.FileSetattrer); ok {
		return h.Setattr(ctx, in, a)
	}
	if usize, ok := in.GetSize(); ok {
		if usize != 0 {
			// We only support setting the size to 0.
			return syscall.ENOTSUP
		}
		err := func() (err error) {
			f, err := n.n.OpenFile(ctx, os.O_WRONLY|os.O_TRUNC)
			if err != nil {
				return errToErrno(err)
			}
			defer file.CloseAndReport(ctx, f, &err)
			w, ok := f.(Writable)
			if !ok {
				return syscall.ENOTSUP
			}
			return w.Flush(ctx)
		}()
		if err != nil {
			return errToErrno(err)
		}
	}
	n.cache.DeleteAll()
	if errno := n.NotifyContent(0 /* offset */, 0 /* len, zero means all */); errno != fs.OK {
		log.Error.Printf("regInode.Setattr %s: error from NotifyContent: %v", n.Path(nil), errno)
		return errToErrno(errno)
	}
	// TODO(josh): Is this the right invalidation, and does it work? Maybe page cache only matters
	// if we set some other flags in open or read to enable it?
	setAttrFromFileInfo(&a.Attr, n.n.Info())
	a.SetTimeout(getCacheTimeout(n.n))
	return fs.OK
}
