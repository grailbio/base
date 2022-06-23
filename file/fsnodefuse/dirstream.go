package fsnodefuse

import (
	"context"
	"io"
	"syscall"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/log"
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
	// prev is the node of the previous entry, i.e. the node of the most recent
	// entry returned by Next.  We cache this node to service LOOKUP operations
	// that go-fuse issues when servicing READDIRPLUS.  See lookupCache.
	prev fsnode.T
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
		d.nextErr = errors.E(err, "fsnodefuse.dirStream")
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
	if d.prev != nil {
		d.dir.readdirplusCache.Drop(d.prev)
	}
	d.prev = next
	d.dir.readdirplusCache.Put(next)
	name := next.Info().Name()
	return fuse.DirEntry{
		Name: name,
		Mode: mode(next),
		Ino:  hashIno(d.dir, name),
	}, fs.OK
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
	if d.prev != nil {
		d.dir.readdirplusCache.Drop(d.prev)
		d.prev = nil
	}
}
