// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package gfilefs

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/grail/biofs/biofseventlog"
	"github.com/grailbio/base/ioctx/fsctx"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/vcontext"
	v23context "v.io/v23/context"
)

// New returns a new parent node rooted at root.  root must be a directory path
// that can be handled by github.com/grailbio/base/file.  name will become the
// name of the returned node.
func New(root, name string) fsnode.Parent {
	return newDirNode(root, name)
}

const fileInfoCacheFor = 1 * time.Hour

func newDirNode(path, name string) fsnode.Parent {
	return dirNode{
		FileInfo: fsnode.NewDirInfo(name).
			WithModePerm(0777).
			WithCacheableFor(fileInfoCacheFor).
			// TODO: Remove after updating fragments to support fsnode.T directly.
			WithSys(path),
		path: path,
	}
}

// dirNode implements fsnode.Parent and represents a directory.
type dirNode struct {
	fsnode.ParentReadOnly
	fsnode.FileInfo
	path string
}

var (
	_ fsnode.Parent    = dirNode{}
	_ fsnode.Cacheable = dirNode{}
)

// Child implements fsnode.Parent.
func (d dirNode) Child(ctx context.Context, name string) (fsnode.T, error) {
	log.Debug.Printf("gfilefs child name=%s", name)
	biofseventlog.UsedFeature("gfilefs.dir.child")
	var (
		path  = file.Join(d.path, name)
		child fsnode.T
	)
	vctx := v23context.FromGoContextWithValues(ctx, vcontext.Background())
	lister := file.List(vctx, path, true /* recursive */)
	// Look for either a file or a directory at this path.  If both exist,
	// assume file is a directory marker.
	// TODO: Consider making base/file API more ergonomic for file and
	// directory name collisions, e.g. by making it easy to systematically
	// shadow one.
	for lister.Scan() {
		if lister.IsDir() || // We've found an exact match, and it's a directory.
			lister.Path() != path { // We're seeing children, so path must be a directory.
			child = newDirNode(path, name)
			break
		}
		child = newFileNode(path, toRegInfo(name, lister.Info()))
	}
	if err := lister.Err(); err != nil {
		return nil, errors.E(err, "scanning", path)
	}
	if child == nil {
		return nil, errors.E(errors.NotExist, path, "not found")
	}
	return child, nil
}

// Children implements fsnode.Parent.
func (d dirNode) Children() fsnode.Iterator {
	biofseventlog.UsedFeature("gfilefs.dir.children")
	return fsnode.NewLazyIterator(d.generateChildren)
}

// AddChildLeaf implements fsnode.Parent.
func (d dirNode) AddChildLeaf(
	ctx context.Context,
	name string,
	flags uint32,
) (fsnode.Leaf, fsctx.File, error) {
	biofseventlog.UsedFeature("gfilefs.dir.addLeaf")
	path := file.Join(d.path, name)
	info := fsnode.NewRegInfo(name).
		WithModePerm(0444).
		WithCacheableFor(fileInfoCacheFor)
	n := newFileNode(path, info)
	f, err := n.OpenFile(ctx, int(flags))
	if err != nil {
		return nil, nil, errors.E(err, "creating file")
	}
	return n, f, nil
}

// AddChildParent implements fsnode.Parent.
func (d dirNode) AddChildParent(_ context.Context, name string) (fsnode.Parent, error) {
	biofseventlog.UsedFeature("gfilefs.dir.addParent")
	// TODO: Consider supporting directories better in base/file, maybe with
	// some kind of directory marker.
	path := file.Join(d.path, name)
	return newDirNode(path, name), nil
}

// RemoveChild implements fsnode.Parent.
func (d dirNode) RemoveChild(ctx context.Context, name string) error {
	biofseventlog.UsedFeature("gfilefs.rmChild")
	return file.Remove(ctx, file.Join(d.path, name))
}

func (d dirNode) FSNodeT() {}

func (d dirNode) generateChildren(ctx context.Context) ([]fsnode.T, error) {
	var (
		// byName is keyed by child name and is used to handle duplicate names
		// we may get when scanning, i.e. if there is a directory and file with
		// the same name (which is possible in S3).
		byName = make(map[string]fsnode.T)
		vctx   = v23context.FromGoContextWithValues(ctx, vcontext.Background())
		lister = file.List(vctx, d.path, false)
	)
	for lister.Scan() {
		var (
			childPath = lister.Path()
			name      = file.Base(childPath)
		)
		// Resolve duplicates by preferring the directory and shadowing the
		// file.  This should be kept consistent with the behavior of Child.
		// We do not expect multiple files or directories with the same name,
		// so behavior of that case is undefined.
		if lister.IsDir() {
			byName[name] = newDirNode(childPath, name)
		} else if _, ok := byName[name]; !ok {
			byName[name] = newFileNode(childPath, toRegInfo(name, lister.Info()))
		}
	}
	if err := lister.Err(); err != nil {
		return nil, errors.E(err, "listing", d.path)
	}
	children := make([]fsnode.T, 0, len(byName))
	for _, child := range byName {
		children = append(children, child)
	}
	return children, nil
}

type fileNode struct {
	// path of the file this node represents.
	path string

	// TODO: Consider expiring this info to pick up external changes (or fix
	// possible inconsistency due to races?).
	info atomic.Value // fsnode.FileInfo
}

var (
	_ (fsnode.Cacheable) = (*fileNode)(nil)
	_ (fsnode.Leaf)      = (*fileNode)(nil)
)

func newFileNode(path string, info fsnode.FileInfo) *fileNode {
	// TODO: Remove after updating fragments to support fsnode.T directly.
	info = info.WithSys(path)
	n := fileNode{path: path}
	n.info.Store(info)
	return &n
}

func (n *fileNode) Info() os.FileInfo {
	return n.fsnodeInfo()
}

func (fileNode) FSNodeT() {}

func (n *fileNode) OpenFile(ctx context.Context, flag int) (fsctx.File, error) {
	biofseventlog.UsedFeature("gfilefs.file.open")
	return OpenFile(ctx, n, flag)
}

func (n *fileNode) CacheableFor() time.Duration {
	return fsnode.CacheableFor(n.fsnodeInfo())
}

func (n *fileNode) fsnodeInfo() fsnode.FileInfo {
	return n.info.Load().(fsnode.FileInfo)
}

func (n *fileNode) setFsnodeInfo(info fsnode.FileInfo) {
	n.info.Store(info)
}

func toRegInfo(name string, info file.Info) fsnode.FileInfo {
	return fsnode.NewRegInfo(name).
		WithModePerm(0666).
		WithSize(info.Size()).
		WithModTime(info.ModTime()).
		WithCacheableFor(fileInfoCacheFor)
}
