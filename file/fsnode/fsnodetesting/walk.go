package fsnodetesting

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/testutil/assert"
	"github.com/stretchr/testify/require"
)

// Walker is a collection of settings.
// TODO: Add (Walker).Walk* variant that inspects FileInfo, too, not just content.
type Walker struct {
	IgnoredNames map[string]struct{}
	// Info makes WalkContents return InfoT recursively. See that function.
	Info bool
}

// T, Parent, and Leaf are aliases to improve readability of fixture definitions.
// InfoT augments T with its FileInfo for tests that want to check mode, size, etc.
type (
	T      = interface{}
	Parent = map[string]T
	Leaf   = []byte

	InfoT = struct {
		fsnode.FileInfo
		T
	}
)

// WalkContents traverses all of node and returns map and []byte objects representing
// parents/directories and leaves/files, respectively.
//
// For example, if node is a Parent with children named a and b that are regular files, and an empty
// subdirectory subdir, returns:
//   Parent{
//   	"a":      Leaf("a's content"),
//   	"b":      Leaf("b's content"),
//   	"subdir": Parent{},
//   }
//
// If w.Info, the returned contents will include fsnode.FileInfo, for example:
//   InfoT{
//   	fsnode.NewDirInfo("parent"),
//   	Parent{
//   		"a": InfoT{
//   			fsnode.NewRegInfo("a").WithSize(11),
//   			Leaf("a's content"),
//   		},
//   		"b": InfoT{
//   			fsnode.NewRegInfo("b").WithModePerm(0755),
//   			Leaf("b's content"),
//   		},
//   		"subdir": InfoT{
//   			fsnode.NewDirInfo("subdir")
//   			Parent{},
//   		},
//   	},
//   }
func (w Walker) WalkContents(ctx context.Context, t testing.TB, node fsnode.T) T {
	switch n := node.(type) {
	case fsnode.Parent:
		dir := make(Parent)
		children, err := fsnode.IterateAll(ctx, n.Children())
		require.NoError(t, err)
		for _, child := range children {
			name := child.Info().Name()
			if _, ok := w.IgnoredNames[name]; ok {
				continue
			}
			_, collision := dir[name]
			require.Falsef(t, collision, "name %q is repeated", name)
			dir[name] = w.WalkContents(ctx, t, child)
		}
		if w.Info {
			return InfoT{fsnode.CopyFileInfo(n.Info()), dir}
		}
		return dir
	case fsnode.Leaf:
		leaf := LeafReadAll(ctx, t, n)
		if w.Info {
			return InfoT{fsnode.CopyFileInfo(n.Info()), leaf}
		}
		return leaf
	}
	require.Failf(t, "invalid node type", "node: %T", node)
	panic("unreachable")
}

func LeafReadAll(ctx context.Context, t testing.TB, n fsnode.Leaf) []byte {
	file, err := fsnode.Open(ctx, n)
	require.NoError(t, err)
	defer func() { assert.NoError(t, file.Close(ctx)) }()
	content, err := ioutil.ReadAll(ioctx.ToStdReader(ctx, file))
	require.NoError(t, err)
	return content
}
