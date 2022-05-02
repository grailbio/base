package fsnodefuse

import (
	"context"
	"crypto/sha512"
	"encoding/binary"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/sync/loadingcache"
	"github.com/grailbio/base/sync/loadingcache/ctxloadingcache"
	"github.com/grailbio/base/writehash"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type dirInode struct {
	fs.Inode
	cache            loadingcache.Map
	readdirplusCache readdirplusCache

	mu sync.Mutex
	n  fsnode.Parent
}

var (
	_ fs.InodeEmbedder = (*dirInode)(nil)

	_ fs.NodeCreater   = (*dirInode)(nil)
	_ fs.NodeGetattrer = (*dirInode)(nil)
	_ fs.NodeLookuper  = (*dirInode)(nil)
	_ fs.NodeReaddirer = (*dirInode)(nil)
	_ fs.NodeSetattrer = (*dirInode)(nil)
	_ fs.NodeUnlinker  = (*dirInode)(nil)
)

func (n *dirInode) Readdir(ctx context.Context) (_ fs.DirStream, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, &n.cache)
	return newDirStream(ctx, n), fs.OK
}

func (n *dirInode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (_ *fs.Inode, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	ctx = ctxloadingcache.With(ctx, &n.cache)
	childFSNode := n.readdirplusCache.Get(name)
	if childFSNode == nil {
		var err error
		childFSNode, err = n.n.Child(ctx, name)
		if err != nil {
			return nil, errToErrno(err)
		}
	}
	childInode := n.GetChild(name)
	if childInode == nil || stableAttr(n, childFSNode) != childInode.StableAttr() {
		childInode = n.newInode(ctx, childFSNode)
	}
	setFSNode(childInode, childFSNode)
	setEntryOut(out, childInode.StableAttr().Ino, childFSNode)
	return childInode, fs.OK
}

func (n *dirInode) Getattr(ctx context.Context, _ fs.FileHandle, a *fuse.AttrOut) (errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	setAttrFromFileInfo(&a.Attr, n.n.Info())
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

	setAttrFromFileInfo(&a.Attr, n.n.Info())
	a.SetTimeout(getCacheTimeout(n.n))
	return fs.OK
}

func (n *dirInode) Create(
	ctx context.Context,
	name string,
	flags uint32,
	mode uint32,
	out *fuse.EntryOut,
) (_ *fs.Inode, _ fs.FileHandle, _ uint32, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if (mode & syscall.S_IFREG) == 0 {
		return nil, nil, 0, syscall.EINVAL
	}
	leaf, f, err := n.n.AddChildLeaf(ctx, name, flags)
	if err != nil {
		return nil, nil, 0, errToErrno(err)
	}
	ino := hashIno(n, leaf.Info().Name())
	embed := &regInode{n: leaf}
	inode := n.NewInode(ctx, embed, fs.StableAttr{Mode: mode, Ino: ino})
	h, err := makeHandle(embed, flags, f)
	return inode, h, 0, errToErrno(err)
}

func (n *dirInode) Unlink(ctx context.Context, name string) syscall.Errno {
	return errToErrno(n.n.RemoveChild(ctx, name))
}

func (n *dirInode) Mkdir(
	ctx context.Context,
	name string,
	mode uint32,
	out *fuse.EntryOut,
) (_ *fs.Inode, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	p, err := n.n.AddChildParent(ctx, name)
	if err != nil {
		return nil, errToErrno(err)
	}
	embed := &dirInode{n: p}
	mode |= syscall.S_IFDIR
	ino := hashIno(n, name)
	inode := n.NewInode(ctx, embed, fs.StableAttr{Mode: mode, Ino: ino})
	setEntryOut(out, ino, p)
	return inode, fs.OK
}

// newInode returns an inode that wraps fsNode.  The type of inode (embedder)
// to create is inferred from the type of fsNode.
func (n *dirInode) newInode(ctx context.Context, fsNode fsnode.T) *fs.Inode {
	var embed fs.InodeEmbedder
	// TODO: Set owner/UID?
	switch fsNode.(type) {
	case fsnode.Parent:
		embed = &dirInode{}
	case fsnode.Leaf:
		embed = &regInode{}
	default:
		log.Panicf("invalid node type: %T", fsNode)
	}
	inode := n.NewInode(ctx, embed, stableAttr(n, fsNode))
	// inode may be an existing inode with an existing embedder.  Regardless,
	// update the underlying fsnode.T.
	setFSNode(inode, fsNode)
	return inode
}

func setEntryOut(out *fuse.EntryOut, ino uint64, n fsnode.T) {
	out.Ino = ino
	setAttrFromFileInfo(&out.Attr, n.Info())
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

func mode(n fsnode.T) uint32 {
	switch n.(type) {
	case fsnode.Parent:
		return syscall.S_IFDIR
	case fsnode.Leaf:
		return syscall.S_IFREG
	default:
		log.Panicf("invalid node type: %T", n)
		panic("unreachable")
	}
}

// readdirplusCache caches nodes for calls to Lookup that go-fuse issues when
// servicing READDIRPLUS.  To handle READDIRPLUS, go-fuse interleaves LOOKUP
// calls for each directory entry.  dirStream populates this cache with the
// last returned entry so that it can be used in Lookup, saving a possibly
// costly (fsnode.Parent).Child call.
type readdirplusCache struct {
	// mu is used to provide exclusive access to the fields below.
	mu sync.Mutex
	// m maps child node names to the set of cached nodes for each name.  The
	// calls to Lookup do not indicate whether they are for a READDIRPLUS, so
	// if there are two dirStream instances which each cached a node for a
	// given name, Lookup will use an arbitrary node in the cache, as we don't
	// know which Lookup is associated with which dirStream.  This might cause
	// transiently stale information but keeps the implementation simple.
	m map[string][]fsnode.T
}

// Put puts a node n in the cache.
func (c *readdirplusCache) Put(n fsnode.T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = make(map[string][]fsnode.T)
	}
	name := n.Info().Name()
	c.m[name] = append(c.m[name], n)
}

// Get gets a node in the cache for the given name.  If no node is cached,
// returns nil.
func (c *readdirplusCache) Get(name string) fsnode.T {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		return nil
	}
	ns, ok := c.m[name]
	if !ok {
		return nil
	}
	return ns[0]
}

// Drop drops the node n from the cache.  n must have been previously added as
// an entry for name using Put.
func (c *readdirplusCache) Drop(n fsnode.T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	name := n.Info().Name()
	ns, _ := c.m[name]
	if len(ns) == 1 {
		delete(c.m, name)
		return
	}
	var dropIndex int
	for i := range ns {
		if n == ns[i] {
			dropIndex = i
			break
		}
	}
	last := len(ns) - 1
	ns[dropIndex] = ns[last]
	ns[last] = nil
	ns = ns[:last]
	c.m[name] = ns
}

func stableAttr(parent fs.InodeEmbedder, n fsnode.T) fs.StableAttr {
	var mode uint32
	switch n.(type) {
	case fsnode.Parent:
		mode |= syscall.S_IFDIR
	case fsnode.Leaf:
		mode |= syscall.S_IFREG
	default:
		log.Panicf("invalid node type: %T", n)
	}
	return fs.StableAttr{
		Mode: mode,
		Ino:  hashIno(parent, n.Info().Name()),
	}
}

func hashParentInoAndName(parentIno uint64, name string) uint64 {
	h := sha512.New()
	writehash.Uint64(h, parentIno)
	writehash.String(h, name)
	return binary.LittleEndian.Uint64(h.Sum(nil)[:8])
}

func hashIno(parent fs.InodeEmbedder, name string) uint64 {
	return hashParentInoAndName(parent.EmbeddedInode().StableAttr().Ino, name)
}
