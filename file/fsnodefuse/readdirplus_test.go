package fsnodefuse

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/testutil"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Set these to lower numbers for easier debugging.
const (
	NumChildren           = 1000
	NumConcurrentReaddirs = 100
)

// TestReaddirplus verifies that servicing a READDIRPLUS request does not
// trigger calls to (fsnode.Parent).Child.  Note that this test uses
// (*os.File).Readdirnames to trigger the READDIRPLUS request.
func TestReaddirplus(t *testing.T) {
	children := makeTestChildren()
	root := newParent("root", children)
	withMounted(t, root, func(mountDir string) {
		err := checkDir(t, children, mountDir)
		require.NoError(t, err)
		assert.Equal(t, int64(0), root.childCalls)
	})
}

// TestReaddirplusConcurrent verifies that servicing many concurrent
// READDIRPLUS requests does not trigger any calls to (fsnode.Parent).Child.
// Note that this test uses (*os.File).Readdirnames to trigger the READDIRPLUS
// requests.
func TestReaddirplusConcurrent(t *testing.T) {
	children := makeTestChildren()
	root := newParent("root", children)
	withMounted(t, root, func(mountDir string) {
		var grp errgroup.Group
		for i := 0; i < NumConcurrentReaddirs; i++ {
			grp.Go(func() error {
				return checkDir(t, children, mountDir)
			})
		}
		require.NoError(t, grp.Wait())
		assert.Equal(t, int64(0), root.childCalls)
	})
}

func makeTestChildren() []fsnode.T {
	children := make([]fsnode.T, NumChildren)
	for i := range children {
		children[i] = fsnode.ConstLeaf(
			fsnode.NewRegInfo(fmt.Sprintf("%04d", i)),
			[]byte{},
		)
	}
	return children
}

// withMounted sets up and tears down a FUSE mount for root.
// f is called with the path where root is mounted.
func withMounted(t *testing.T, root fsnode.T, f func(rootPath string)) {
	mountDir, cleanUp := testutil.TempDir(t, "", "fsnodefuse-testreaddirplus")
	defer cleanUp()
	server, err := fs.Mount(mountDir, NewRoot(root), &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:        "test",
			DisableXAttrs: true,
		},
	})
	require.NoError(t, err, "mounting %q", mountDir)
	defer func() {
		assert.NoError(t, server.Unmount(),
			"unmount of FUSE mounted at %q failed; may need manual cleanup",
			mountDir,
		)
	}()
	f(mountDir)
}

func checkDir(t *testing.T, children []fsnode.T, path string) (err error) {
	want := make(map[string]struct{})
	for _, c := range children {
		want[c.Name()] = struct{}{}
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { assert.NoError(t, f.Close()) }()
	// Use Readdirnames instead of Readdir because Readdir adds an extra call
	// lstat outside of the READDIRPLUS operation.
	names, err := f.Readdirnames(0)
	got := make(map[string]struct{})
	for _, name := range names {
		got[name] = struct{}{}
	}
	// Sanity check that the names of the entries match the children.
	assert.Equal(t, want, got)
	return err
}

type parent struct {
	fsnode.Parent
	childCalls int64
}

func (p *parent) Child(ctx context.Context, name string) (fsnode.T, error) {
	atomic.AddInt64(&p.childCalls, 1)
	return p.Parent.Child(ctx, name)
}

// CacheableFor implements fsnode.Cacheable.
func (p parent) CacheableFor() time.Duration {
	return fsnode.CacheableFor(p.Parent)
}

func newParent(name string, children []fsnode.T) *parent {
	return &parent{
		Parent: fsnode.NewParent(
			fsnode.NewDirInfo("root"),
			fsnode.ConstChildren(children...),
		),
	}
}
