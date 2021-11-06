package fsnodefuse

import (
	"context"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"io"
	"syscall"

	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/writehash"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type dirStream struct {
	ctx     context.Context
	dir     *dirInode
	iter    fsnode.Iterator
	eof     bool
	next    fsnode.T
	nextErr error
	// previousInode is the inode of the previous entry, i.e. the most recent
	// entry returned by Next.  We hold a reference to service LOOKUP
	// operations that go-fuse issues when servicing READDIRPLUS.  See
	// dirStreamUsage.
	previousInode *fs.Inode
}

func newDirStream(ctx context.Context, dir *dirInode) *dirStream {
	return &dirStream{
		ctx:  ctx,
		dir:  dir,
		iter: dir.n.Children(),
	}
}

func (d *dirStream) HasNext() bool {
	if d.next != nil || d.nextErr != nil {
		return true
	}
	defer handlePanicErr(&d.nextErr)
	if d.eof {
		return false
	}
	next, err := d.iter.Next(d.ctx)
	if err == io.EOF {
		d.eof = true
		return false
	} else if err != nil {
		d.nextErr = fmt.Errorf("fsnodefuse.dirStream: %w", err)
		// Return true here so Next() has a chance to return d.nextErr.
		return true
	}
	d.next = next
	return true
}

func (d *dirStream) Next() (_ fuse.DirEntry, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if err := d.nextErr; err != nil {
		return fuse.DirEntry{}, errToErrno(err)
	}
	next := d.next
	d.next = nil
	inode, entry, err := d.dir.makeChild(d.ctx, next)
	if err != nil {
		return fuse.DirEntry{}, errToErrno(err)
	}
	// We are passing overwrite == true, so the return value is meaningless.
	_ = d.dir.AddChild(entry.Name, inode, true)
	d.setPreviousInode(inode)
	return entry, fs.OK
}

func (d *dirStream) Close() {
	var err error
	defer handlePanicErr(&err)
	defer func() {
		if err != nil {
			log.Error.Printf("fsnodefuse.dirStream: error on close: %v", err)
		}
	}()
	err = d.iter.Close(d.ctx)
	d.iter = nil
	d.next = nil
	d.clearPreviousInode()
}

func (d *dirStream) setPreviousInode(n *fs.Inode) {
	d.clearPreviousInode()
	d.previousInode = n
	d.previousInode.Operations().(inodeEmbedder).AddRef()
}

func (d *dirStream) clearPreviousInode() {
	if d.previousInode == nil {
		return
	}
	d.previousInode.Operations().(inodeEmbedder).DropRef()
	d.previousInode = nil
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
