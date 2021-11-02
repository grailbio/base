package fsnodefuse

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/grailbio/base/errors"
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
	_ inodeEmbedder = (*dirInode)(nil)

	_ fs.NodeReaddirer = (*dirInode)(nil)
	_ fs.NodeLookuper  = (*dirInode)(nil)
	_ fs.NodeGetattrer = (*dirInode)(nil)
	_ fs.NodeSetattrer = (*dirInode)(nil)
)

// fsNode implements inodeEmbedder.
func (n *dirInode) fsNode() fsnode.T {
	return n.n
}

func (n *dirInode) Readdir(ctx context.Context) (_ fs.DirStream, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, &n.cache)
	return newDirStream(ctx, n), fs.OK
}

func (n *dirInode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (_ *fs.Inode, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if childInode := n.GetChild(name); childInode != nil {
		embed := childInode.Operations().(inodeEmbedder)
		setEntryOut(out, childInode.StableAttr().Ino, embed.fsNode())
		return childInode, fs.OK
	}
	ctx = ctxloadingcache.With(ctx, &n.cache)
	child, err := n.n.Child(ctx, name)
	if err != nil {
		return nil, errToErrno(err)
	}
	entry, childInode, err := n.makeChild(ctx, child)
	if err != nil {
		return nil, errToErrno(err)
	}
	setEntryOut(out, entry.Ino, child)
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

func (n *dirInode) makeChild(
	ctx context.Context,
	fsNode fsnode.T,
) (fuse.DirEntry, *fs.Inode, error) {
	var (
		name  = fsNode.Name()
		ino   = hashParentInoAndName(n.StableAttr().Ino, name)
		embed inodeEmbedder
		mode  uint32
	)
	// TODO: Set owner/UID?
	switch fsNode := fsNode.(type) {
	case fsnode.Parent:
		embed = &dirInode{n: fsNode}
		mode |= syscall.S_IFDIR
	case fsnode.Leaf:
		embed = &regInode{n: fsNode}
		mode |= syscall.S_IFREG
	default:
		log.Error.Printf("BUG: invalid node type: %T", fsNode)
		return fuse.DirEntry{}, nil, errors.E(errors.Invalid)
	}
	var (
		entry = fuse.DirEntry{Name: name, Mode: mode, Ino: ino}
		inode = n.NewInode(ctx, embed, fs.StableAttr{Mode: mode, Ino: ino})
	)
	return entry, inode, nil
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
