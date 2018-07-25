package loopbackfs

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/ioctx/fsctx"
	"github.com/grailbio/base/ioctx/spliceio"
)

// New returns an fsnode.T representing a path on the local filesystem.
// TODO: Replace this with a generic io/fs.FS wrapper around os.DirFS, after upgrading Go.
func New(name string, path string) (fsnode.T, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	node := newT(name, info, path)
	if node == nil {
		return nil, os.ErrInvalid
	}
	return node, nil
}

func newT(name string, info os.FileInfo, path string) fsnode.T {
	switch info.Mode() & os.ModeType {
	case os.ModeDir:
		// Temporary hack: record the original path so we can peek at it.
		// TODO: Eventually, adapt our libraries to operate on FS's directly.
		// TODO: Consider preserving executable bits. But, copying permissions without also checking
		// owner UID/GID may not make sense.
		info := fsnode.NewDirInfo(name).WithModTime(info.ModTime()).WithSys(path).
			WithCacheableFor(time.Hour)
		return parent{dir: path, FileInfo: info}
	case 0:
		info := fsnode.NewRegInfo(name).WithModTime(info.ModTime()).WithSys(path).
			WithCacheableFor(time.Hour)
		return leaf{path, info}
	}
	return nil
}

type parent struct {
	fsnode.ParentReadOnly
	dir string
	fsnode.FileInfo
}

var _ fsnode.Parent = parent{}

func (p parent) FSNodeT() {}

func (p parent) Child(_ context.Context, name string) (fsnode.T, error) {
	return New(name, path.Join(p.dir, name))
}

type iterator struct {
	dir      string
	fetched  bool
	delegate fsnode.Iterator
}

func (p parent) Children() fsnode.Iterator { return &iterator{dir: p.dir} }

func (it *iterator) ensureFetched() error {
	if it.fetched {
		if it.delegate == nil {
			return os.ErrClosed
		}
		return nil
	}
	entries, err := ioutil.ReadDir(it.dir)
	if err != nil {
		return err
	}
	nodes := make([]fsnode.T, 0, len(entries))
	for _, info := range entries {
		fullPath := path.Join(it.dir, info.Name())
		node := newT(info.Name(), info, fullPath)
		if node == nil {
			continue
		}
		nodes = append(nodes, node)
	}
	it.fetched = true
	it.delegate = fsnode.NewIterator(nodes...)
	return nil
}

func (it *iterator) Next(ctx context.Context) (fsnode.T, error) {
	if err := it.ensureFetched(); err != nil {
		return nil, err
	}
	return it.delegate.Next(ctx)
}

func (it *iterator) Close(context.Context) error {
	it.fetched = true
	it.delegate = nil
	return nil
}

type leaf struct {
	path string
	fsnode.FileInfo
}

var _ fsnode.Leaf = leaf{}

func (l leaf) FSNodeT() {}

func (l leaf) OpenFile(context.Context, int) (fsctx.File, error) {
	file, err := os.Open(l.path)
	if err != nil {
		return nil, err
	}
	return (*spliceio.OSFile)(file), nil
}
