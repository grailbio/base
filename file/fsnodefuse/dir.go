package fsnodefuse

import (
	"context"
	"os"
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
	n     fsnode.Parent
	cache loadingcache.Map
}

var (
	_ fs.NodeReaddirer = (*dirInode)(nil)
	_ fs.NodeLookuper  = (*dirInode)(nil)
	_ fs.NodeGetattrer = (*dirInode)(nil)
	_ fs.NodeSetattrer = (*dirInode)(nil)
)

func (n *dirInode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	ctx = ctxloadingcache.With(ctx, &n.cache)
	return newDirStream(ctx, n.StableAttr().Ino, n.n.Children()), fs.OK
}

func (n *dirInode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (_ *fs.Inode, errno syscall.Errno) {
	ctx = ctxloadingcache.With(ctx, &n.cache)

	child, err := n.n.Child(ctx, name)
	if err != nil {
		return nil, errToErrno(err)
	}
	entry := fsEntryToFuseEntry(n.StableAttr().Ino, name, child.IsDir())
	out.Ino = entry.Ino
	setAttrFromFileInfo(&out.Attr, child)
	cacheTimeout := getCacheTimeout(child)
	out.SetEntryTimeout(cacheTimeout)
	out.SetAttrTimeout(cacheTimeout)
	// TODO: Set owner/UID?
	var embed fs.InodeEmbedder
	switch node := child.(type) {
	case fsnode.Parent:
		embed = &dirInode{n: node}
	case fsnode.Leaf:
		embed = &regInode{n: node}
	default:
		log.Error.Printf("BUG: invalid node type: %T", child)
		return nil, syscall.ENOENT
	}
	return n.NewInode(ctx, embed, fs.StableAttr{Mode: entry.Mode, Ino: entry.Ino}), fs.OK
}

func (n *dirInode) Getattr(_ context.Context, _ fs.FileHandle, a *fuse.AttrOut) syscall.Errno {
	setAttrFromFileInfo(&a.Attr, n.n)
	a.SetTimeout(getCacheTimeout(n.n))
	return fs.OK
}

func (n *dirInode) Setattr(_ context.Context, _ fs.FileHandle, _ *fuse.SetAttrIn, a *fuse.AttrOut) syscall.Errno {
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

func setAttrFromFileInfo(a *fuse.Attr, info os.FileInfo) {
	if info.IsDir() {
		a.Mode |= syscall.S_IFDIR
	} else {
		a.Mode |= syscall.S_IFREG
	}
	a.Mode |= uint32(info.Mode() & os.ModePerm)
	a.Size = uint64(info.Size())
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
