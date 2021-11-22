package fsnodefuse

import (
	"context"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/sync/loadingcache"
	"github.com/grailbio/base/sync/loadingcache/ctxloadingcache"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type dirInode struct {
	fs.Inode
	dirStreamUsageImpl
	cache loadingcache.Map

	mu sync.Mutex
	n  fsnode.Parent
}

var (
	_ inodeEmbedder = (*dirInode)(nil)

	_ fs.NodeReaddirer = (*dirInode)(nil)
	_ fs.NodeLookuper  = (*dirInode)(nil)
	_ fs.NodeGetattrer = (*dirInode)(nil)
	_ fs.NodeSetattrer = (*dirInode)(nil)
)

func (n *dirInode) Readdir(ctx context.Context) (_ fs.DirStream, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, &n.cache)
	return newDirStream(ctx, n), fs.OK
}

func (n *dirInode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (_ *fs.Inode, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, &n.cache)
	if childInode := n.GetChild(name); childInode != nil {
		if embed := childInode.Operations().(inodeEmbedder); embed.PreviousOfAnyDirStream() {
			// We can reuse the existing inode without calling (Parent).Child.
			// See dirStreamUsage.
			var childFSNode fsnode.T
			switch embed := embed.(type) {
			case *dirInode:
				embed.mu.Lock()
				childFSNode = embed.n
				embed.mu.Unlock()
			case *regInode:
				embed.mu.Lock()
				childFSNode = embed.n
				embed.mu.Unlock()
			}
			setEntryOut(out, childInode.StableAttr().Ino, childFSNode)
			return childInode, fs.OK
		}
		// We need to call (Parent).Child to re-fetch and ensure the inode is
		// current.
		childFSNode, err := n.n.Child(ctx, name)
		if err != nil {
			return nil, errToErrno(err)
		}
		setFSNode(childInode, childFSNode)
		setEntryOut(out, childInode.StableAttr().Ino, childFSNode)
		return childInode, fs.OK
	}
	// There is no existing child inode, so we construct it.
	childFSNode, err := n.n.Child(ctx, name)
	if err != nil {
		return nil, errToErrno(err)
	}
	childInode := n.newInode(ctx, childFSNode)
	setEntryOut(out, childInode.StableAttr().Ino, childFSNode)
	return childInode, fs.OK
}

func (n *dirInode) Getattr(ctx context.Context, _ fs.FileHandle, a *fuse.AttrOut) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	setAttrFromFileInfo(&a.Attr, n.n)
	a.SetTimeout(getCacheTimeout(n.n))
	return fs.OK
}

func (n *dirInode) Setattr(ctx context.Context, _ fs.FileHandle, _ *fuse.SetAttrIn, a *fuse.AttrOut) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	n.cache.DeleteAll()

	// To avoid deadlock we must notify invalidations while not holding certain inode locks.
	// See: https://github.com/libfuse/libfuse/blob/d709c24cbd9e1041264c551c2a4445e654eaf429/include/fuse_lowlevel.h#L1654-L1661
	// We're ok with best-effort execution of the invalidation so a goroutine conveniently avoids locks.
	children := n.Children()
	go func() {
		for childName, child := range children {
			// TODO: Consider merely NotifyEntry instead of NotifyDelete.
			// Both force a Lookup on the next access, as desired. However, NotifyDelete also
			// deletes the child inode immediately which has UX consequences. For example, if a
			// user's shell is currently working in that directory, after NotifyDelete they may
			// see shell operations fail (similar to what they might see if they `git checkout` a
			// branch that doesn't include the current working directory). NotifyEntry avoids those
			// errors but may introduce inconsistency (that shell will remain using the old inode
			// and its stale contents), which may be confusing.
			// TODO: josh@ is not sure about this inconsistency thing.
			if errno := n.NotifyDelete(childName, child); errno != fs.OK {
				log.Error.Printf("dirInode.Setattr %s: error from NotifyDelete %s: %v", n.Path(nil), childName, errno)
			}
		}
	}()

	setAttrFromFileInfo(&a.Attr, n.n)
	a.SetTimeout(getCacheTimeout(n.n))
	return fs.OK
}

// newInode returns an inode that wraps fsNode.  The type of inode (embedder)
// to create is inferred from the type of fsNode.
func (n *dirInode) newInode(ctx context.Context, fsNode fsnode.T) *fs.Inode {
	var (
		embed inodeEmbedder
		mode  uint32
	)
	// TODO: Set owner/UID?
	switch fsNode.(type) {
	case fsnode.Parent:
		embed = &dirInode{}
		mode |= syscall.S_IFDIR
	case fsnode.Leaf:
		embed = &regInode{}
		mode |= syscall.S_IFREG
	default:
		log.Panicf("invalid node type: %T", fsNode)
	}
	var (
		ino   = hashIno(n, fsNode.Name())
		inode = n.NewInode(ctx, embed, fs.StableAttr{Mode: mode, Ino: ino})
	)
	// inode may be an existing inode with an existing embedder.  Regardless,
	// update the underlying fsnode.T.
	setFSNode(inode, fsNode)
	return inode
}

func setEntryOut(out *fuse.EntryOut, ino uint64, n fsnode.T) {
	out.Ino = ino
	setAttrFromFileInfo(&out.Attr, n)
	cacheTimeout := getCacheTimeout(n)
	out.SetEntryTimeout(cacheTimeout)
	out.SetAttrTimeout(cacheTimeout)
}

func setAttrFromFileInfo(a *fuse.Attr, info os.FileInfo) {
	if info.IsDir() {
		a.Mode |= syscall.S_IFDIR
	} else {
		a.Mode |= syscall.S_IFREG
	}
	a.Mode |= uint32(info.Mode() & os.ModePerm)
	a.Size = uint64(info.Size())
	a.Blocks = a.Size / blockSize
	// We want to encourage large reads to reduce syscall overhead. FUSE has a 128 KiB read
	// size limit anyway.
	// TODO: Is there a better way to set this, in case size limits ever change?
	setBlockSize(a, 128*1024)
	if mod := info.ModTime(); !mod.IsZero() {
		a.SetTimes(nil, &mod, nil)
	}
}

func getCacheTimeout(any interface{}) time.Duration {
	cacheableFor := fsnode.CacheableFor(any)
	if cacheableFor < 0 {
		return 365 * 24 * time.Hour
	}
	return cacheableFor
}
