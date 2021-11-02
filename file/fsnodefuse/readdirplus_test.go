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

// TestReaddirplus verifies that servicing a READDIRPLUS request does not
// trigger calls to (fsnode.Parent).Child.  Note that this test uses
// (*os.File).Readdir to trigger the READDIRPLUS request.
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
// Note that this test uses (*os.File).Readdir to trigger the READDIRPLUS
// requests.
func TestReaddirplusConcurrent(t *testing.T) {
	children := makeTestChildren()
	root := newParent("root", children)
	withMounted(t, root, func(mountDir string) {
		const Nreaddirs = 100
		var grp errgroup.Group
		for i := 0; i < Nreaddirs; i++ {
			grp.Go(func() error {
				return checkDir(t, children, mountDir)
			})
		}
		require.NoError(t, grp.Wait())
		assert.Equal(t, int64(0), root.childCalls)
	})
}

func makeTestChildren() []fsnode.T {
	const Nchildren = 2
	children := make([]fsnode.T, Nchildren)
	for i := range children {
		children[i] = fsnode.ConstLeaf(
			fsnode.NewRegInfo(fmt.Sprintf("%04d", i)),
			[]byte{},
		)
	}
	return children
}

func withMounted(t *testing.T, root fsnode.T, f func(string)) {
	mountDir, cleanUp := testutil.TempDir(t, "", "fsnodefuse-"+t.Name())
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
	infos, err := f.Readdir(0)
	got := make(map[string]struct{})
	for _, info := range infos {
		got[info.Name()] = struct{}{}
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
			fsnode.NewDirInfo("root").WithCacheableFor(1*time.Hour),
			fsnode.ConstChildren(children...),
		),
	}
}
