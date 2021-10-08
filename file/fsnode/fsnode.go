// fsnode represents a filesystem as a directed graph (probably a tree for many implementations).
// Directories are nodes with out edges (children). Files are nodes without.
//
// fsnode.T is designed for incremental iteration. Callers can step through the graph one link
// at a time (Parent.Child) or one level at a time (Parent.Children). In general, implementations
// should do incremental work for each step. See also: Cacheable.
//
// Compared to fs.FS:
//   * Leaf explicitly models an unopened file. fs users have to choose their own representation,
//     like the pair (fs.FS, name string) or func Open(...).
//   * Graph traversal (that is, directory listing) uses the one node type, rather than a separate
//     one (like fs.DirEntry). Callers can access "all cheaply available FileInfo" during listing
//     or can Open nodes if they want completeness at higher cost.
//   * Parent offers one, explicit way of traversing the graph. fs.FS has optional ReadDirFS or
//     callers can Open(".") and see if ReadDirFile is returned. (fs.ReadDir unifies these but also
//     disallows pagination).
//   * Only supports directories and files. fs.FS supports much more. TODO: Add symlinks?
//   * fs.FS.Open naturally allows "jumping" several levels deep without step-by-step traversal.
//     (See Parent.Child) for performance note.
package fsnode

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/ioctx/fsctx"
)

type (
	// T is a Parent or Leaf. A T that is not either of those is invalid.
	T interface {
		// FileInfo provides immediately-available information. A subset of fields must be accurate:
		//   Name
		//   Mode&os.ModeType
		//   IsDir
		// The rest can be zero values if they're not immediately available.
		// Implementations may find FileInfo (in this package) convenient for embedding or use
		// in public API.
		//
		// Leaf.Open().Stat() gets complete information. That returned FileInfo must have the same
		// values for the fields listed above. The others can change if better information is
		// available.
		// TODO: Specify something about embedded FileInfo updating after a Stat call?
		os.FileInfo
		// FSNodeT distinguishes T from os.FileInfo. It does nothing.
		FSNodeT()
	}
	// Parent is a T that has zero or more child Ts.
	Parent interface {
		// T.FileInfo must be consistent with directory (mode and IsDir).
		T
		// Child returns the named child. Returns nil, os.ErrNotExist if no such child exists.
		// name is not a path and must not contain '/'. It must satisfy fs.ValidPath, too.
		//
		// In some implementations, Child lookup may be relatively expensive and implementations
		// may want to reduce the cost of accessing a deeply-nested node. They may make all Child()
		// requests succeed immediately and then return path errors from listing or Leaf.Open
		// operations for the earlier path segment.
		Child(_ context.Context, name string) (T, error)
		// Children returns an iterator that can list all children.
		// Children takes no Context; it's expected to simply construct a view and return errors to
		// callers when they choose an Iterator operation.
		Children() Iterator
	}
	// Iterator yields child nodes iteratively.
	//
	// Users must serialize their own method calls. No calls can be made after Close().
	// TODO: Do we need Stat here, maybe to update directory mode?
	Iterator interface {
		// Next gets the next node. Must return (nil, io.EOF) at the end, not (non-nil, io.EOF).
		Next(context.Context) (T, error)
		// Close frees resources.
		Close(context.Context) error
	}
	// Leaf is a T corresponding to a fsctx.File. It can be opened any number of times and must
	// allow concurrent calls (it may lock internally if necessary).
	Leaf interface {
		// T.FileInfo must be consistent with regular file (mode and !IsDir).
		T
		// Open opens the file. File.Stat()'s result must be consistent with Leaf.FileInfo;
		// see T.FileInfo.
		Open(context.Context) (fsctx.File, error)
	}

	// Cacheable optionally lets users make use of caching. The cacheable data depends on
	// which type Cacheable is defined on:
	//   * On any T, FileInfo.
	//   * On an fsctx.File, the FileInfo and contents.
	//
	// Common T implementations are expected to be "views" of remote data sources not under
	// our exclusive control (like local filesystem or S3). As such, callers should generally
	// expect best-effort consistency, regardless of caching.
	Cacheable interface {
		// CacheableFor is the maximum allowed cache time.
		// Zero means don't cache. Negative means cache forever.
		// TODO: Make this a non-Duration type to avoid confusion with negatives?
		CacheableFor() time.Duration
	}
	cacheableFor struct{ time.Duration }
)

const CacheForever = time.Duration(-1)

// CacheableFor returns the configured cache time if obj is Cacheable, otherwise returns default 0.
func CacheableFor(obj interface{}) time.Duration {
	cacheable, ok := obj.(Cacheable)
	if !ok {
		return 0
	}
	return cacheable.CacheableFor()
}
func NewCacheable(d time.Duration) Cacheable       { return cacheableFor{d} }
func (c cacheableFor) CacheableFor() time.Duration { return c.Duration }

// IterateFull reads the full len(dst) nodes from Iterator. If actual number read is less than
// len(dst), error is non-nil. Error is io.EOF for EOF. Unlike io.ReadFull, this doesn't return
// io.ErrUnexpectedEOF (unless iter does).
func IterateFull(ctx context.Context, iter Iterator, dst []T) (int, error) {
	for i := range dst {
		var err error
		dst[i], err = iter.Next(ctx)
		if err != nil {
			if err == io.EOF && dst[i] != nil {
				return i, iteratorEOFError(iter)
			}
			return i, err
		}
	}
	return len(dst), nil
}

// IterateAll reads iter until EOF. Returns nil error on success, not io.EOF (like io.ReadAll).
func IterateAll(ctx context.Context, iter Iterator) ([]T, error) {
	var dst []T
	for {
		node, err := iter.Next(ctx)
		if err != nil {
			if err == io.EOF {
				if node != nil {
					return dst, iteratorEOFError(iter)
				}
				return dst, nil
			}
			return dst, err
		}
		dst = append(dst, node)
	}
}

func iteratorEOFError(iter Iterator) error {
	return errors.E(errors.Precondition, fmt.Sprintf("BUG: iterator.Next (%T) returned element+EOF", iter))
}
