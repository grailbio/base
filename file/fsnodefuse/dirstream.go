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
	entries []fsnode.T
	// prev is the node of the previous entry, i.e. the node of the most recent
	// entry returned by Next.  We cache this node to service LOOKUP operations
	// that go-fuse issues when servicing READDIRPLUS.  See lookupCache.
	prev fsnode.T
}

var _ fs.DirStream = (*dirStream)(nil)

// newDirStream returns a dirStream whose entries are the children of dir.
// Children are loaded eagerly so that any errors are reported before any
// entries are returned by the stream.  If Next returns a non-OK Errno after
// a call that returned an OK Errno, the READDIR operation returns an EIO,
// regardless of the returned Errno.  See
// https://github.com/hanwen/go-fuse/issues/436 .
func newDirStream(
	ctx context.Context,
	dir *dirInode,
) (_ *dirStream, err error) {
	var (
		entries []fsnode.T
		iter    = dir.n.Children()
	)
	defer errors.CleanUpCtx(ctx, iter.Close, &err)
	for {
		n, err := iter.Next(ctx)
		if err == io.EOF {
			return &dirStream{
				ctx:     ctx,
				dir:     dir,
				entries: entries,
			}, nil
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, n)
	}
}

func (d *dirStream) HasNext() bool {
	return len(d.entries) != 0
}

func (d *dirStream) Next() (_ fuse.DirEntry, errno syscall.Errno) {
	defer handlePanicErrno(&errno)
	var next fsnode.T
	next, d.entries = d.entries[0], d.entries[1:]
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
	if d.prev != nil {
		d.dir.readdirplusCache.Drop(d.prev)
		d.prev = nil
	}
}
