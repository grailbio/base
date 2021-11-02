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
	ctx        context.Context
	dir        *dirInode
	iter       fsnode.Iterator
	eof        bool
	current    fsnode.T
	currentErr error
}

func newDirStream(ctx context.Context, dir *dirInode) *dirStream {
	return &dirStream{
		ctx:  ctx,
		dir:  dir,
		iter: dir.n.Children(),
	}
}

func (d *dirStream) HasNext() bool {
	if d.current != nil || d.currentErr != nil {
		return true
	}
	defer handlePanicErr(&d.currentErr)
	if d.eof {
		return false
	}
	next, err := d.iter.Next(d.ctx)
	if err == io.EOF {
		d.eof = true
		return false
	} else if err != nil {
		d.currentErr = fmt.Errorf("fsnodefuse.dirStream: %w", err)
		return false
	}
	d.current = next
	return true
}

func (d *dirStream) Next() (_ fuse.DirEntry, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	if err := d.currentErr; err != nil {
		return fuse.DirEntry{}, errToErrno(err)
	}
	current := d.current
	d.current = nil
	entry, childInode, err := d.dir.makeChild(d.ctx, current)
	if err != nil {
		return fuse.DirEntry{}, errToErrno(err)
	}
	d.dir.AddChild(entry.Name, childInode, true)
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
	d.current = nil
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
