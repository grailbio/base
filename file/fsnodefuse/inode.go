package fsnodefuse

import (
	"sync/atomic"

	"github.com/hanwen/go-fuse/v2/fs"
)

// inodeEmbedder is implemented by all fsnodefuse inode types.
type inodeEmbedder interface {
	fs.InodeEmbedder
	dirStreamUsage
}

// dirStreamUsage tracks the usage of the implementing embedder in DirStreams.
// It is used to decide whether an inode can be reused to service LOOKUP
// operations.  To handle READDIRPLUS, go-fuse interleaves LOOKUP calls for
// each directory entry.  We allow the embedder associated with the previous
// directory entry to be used in LOOKUP to avoid a possibly costly
// (fsnode).Child call.
//
// Because an embedder/inode can be the previous entry in multiple DirStreams,
// we maintain a reference count.
//
// It is possible for the inode to be forgotten, e.g. when the kernel is low on
// memory, before the LOOKUP call.  If this happens, LOOKUP will need to make
// the (fsnode).Child call.  This seems to happen rarely, if at all, in
// practice.
type dirStreamUsage interface {
	// AddRef adds a single reference to this embedder.  It must be eventually
	// followed by a DropRef.
	AddRef()
	// DropRef drops a single reference to this embedder.
	DropRef()
	// PreviousOfAnyDirStream returns true iff the embedder represents the
	// previous entry returned by any outstanding DirStream.
	PreviousOfAnyDirStream() bool
}

// dirStreamUsageImpl implements dirStreamUsage and is meant to be embedded as
// a field in our embedder implementations.
type dirStreamUsageImpl struct {
	nRef int32
}

func (m *dirStreamUsageImpl) AddRef() {
	_ = atomic.AddInt32(&m.nRef, 1)
}

func (m *dirStreamUsageImpl) DropRef() {
	if n := atomic.AddInt32(&m.nRef, -1); n < 0 {
		panic("negative reference count; unmatched drop")
	}
}

func (m *dirStreamUsageImpl) PreviousOfAnyDirStream() bool {
	return atomic.LoadInt32(&m.nRef) > 0
}
