package fsnodefuse

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"sync/atomic"
	"testing"

	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/ioctx/fsctx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPanic* panic while handling a FUSE operation, then repeat the operation to make
// sure the mount isn't broken.
// TODO: Test operations exhaustively, maybe with something like fuzzing.

func TestPanicOpen(t *testing.T) {
	const success = "success"
	var (
		info     = fsnode.NewRegInfo("panicOnce")
		panicked int32
	)
	root := fsnode.NewParent(fsnode.NewDirInfo("root"),
		fsnode.ConstChildren(
			fsnode.FuncLeaf(
				info,
				func(ctx context.Context, flag int) (fsctx.File, error) {
					if atomic.AddInt32(&panicked, 1) == 1 {
						panic("it's a panic!")
					}
					return fsnode.Open(ctx, fsnode.ConstLeaf(info, []byte(success)))
				},
			),
		),
	)
	withMounted(t, root, func(mountDir string) {
		_, err := os.Open(path.Join(mountDir, "panicOnce"))
		require.Error(t, err)
		f, err := os.Open(path.Join(mountDir, "panicOnce"))
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		content, err := ioutil.ReadAll(f)
		require.NoError(t, err)
		assert.Equal(t, success, string(content))
	})
}

func TestPanicList(t *testing.T) {
	const success = "success"
	var panicked int32
	root := fsnode.NewParent(fsnode.NewDirInfo("root"),
		fsnode.FuncChildren(func(context.Context) ([]fsnode.T, error) {
			if atomic.AddInt32(&panicked, 1) == 1 {
				panic("it's a panic!")
			}
			return []fsnode.T{
				fsnode.ConstLeaf(fsnode.NewRegInfo(success), nil),
			}, nil
		}),
	)
	withMounted(t, root, func(mountDir string) {
		dir, err := os.Open(mountDir)
		require.NoError(t, err)
		ents, _ := dir.Readdirnames(0)
		// It seems like Readdirnames returns nil error despite the panic.
		// TODO: Confirm this is expected.
		assert.Empty(t, ents)
		require.NoError(t, dir.Close())
		dir, err = os.Open(mountDir)
		require.NoError(t, err)
		defer func() { require.NoError(t, dir.Close()) }()
		ents, err = dir.Readdirnames(0)
		assert.NoError(t, err)
		assert.Equal(t, []string{success}, ents)
	})
}
