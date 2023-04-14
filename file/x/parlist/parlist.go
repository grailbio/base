package parlist

import (
	"context"

	"github.com/grailbio/base/file"
)

type ShardLister interface {
	// NewShard adds a new worker shard, or nil if not supported.
	NewShard() BatchLister
}

type BatchLister interface {
	// Note: !more means client should stop Scan()-ing. len(batch) == 0 does not imply !more.
	Scan(context.Context) (batch []Info, more bool)
	Err() error
}

type Info interface {
	Path() string
	// Info is nil for directories. Note: file.ETagged is related.
	file.Info
	IsDir() bool
}

type Implementation interface {
	ListBatch(_ context.Context, path string, _ ListOpts) BatchLister
}

type ListOpts struct {
	Recursive bool
	// StartAfter is relative to the path.
	StartAfter string
	// BatchSizeHint recommends a batch size to implementations. They're not guaranteed to use it.
	BatchSizeHint int
}

type batchAdapter struct {
	ctx       context.Context
	batch     BatchLister
	batchDone bool
	current   Info
	next      []Info
}

// NewAdapter returns a file.Lister that reads batch.
func NewAdapter(ctx context.Context, batch BatchLister) file.Lister {
	return &batchAdapter{ctx: ctx, batch: batch}
}

func (l *batchAdapter) Scan() bool {
	for {
		if l.batch.Err() != nil {
			return false
		}
		if len(l.next) > 0 {
			l.current, l.next = l.next[0], l.next[1:]
			return true
		}
		if l.batchDone {
			return false
		}
		var more bool
		l.next, more = l.batch.Scan(l.ctx)
		l.batchDone = !more
	}
}

func (l *batchAdapter) Err() error { return l.batch.Err() }

func (l *batchAdapter) Path() string    { return l.current.Path() }
func (l *batchAdapter) Info() file.Info { return l.current }
func (l *batchAdapter) IsDir() bool     { return l.current.IsDir() }

type errLister struct{ err error }

func (e errLister) NewShard() BatchLister               { return e }
func (e errLister) Scan(context.Context) ([]Info, bool) { return nil, false }
func (e errLister) Err() error                          { return e.err }
