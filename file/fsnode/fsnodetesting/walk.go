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
}

// T, Parent, and Leaf are aliases to improve readability of fixture definitions.
type (
	T      = interface{}
	Parent = map[string]T
	Leaf   = []byte
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
func (w Walker) WalkContents(ctx context.Context, t testing.TB, node fsnode.T) T {
	switch n := node.(type) {
	case fsnode.Parent:
		dir := make(Parent)
		children, err := fsnode.IterateAll(ctx, n.Children())
		require.NoError(t, err)
		for _, child := range children {
			if _, ok := w.IgnoredNames[child.Name()]; ok {
				continue
			}
			_, collision := dir[child.Name()]
			require.Falsef(t, collision, "name %q is repeated", child.Name())
			dir[child.Name()] = w.WalkContents(ctx, t, child)
		}
		return dir
	case fsnode.Leaf:
		return LeafReadAll(ctx, t, n)
	}
	require.Failf(t, "invalid node type", "node: %T", node)
	panic("unreachable")
}

func LeafReadAll(ctx context.Context, t testing.TB, n fsnode.Leaf) []byte {
	file, err := n.Open(ctx)
	require.NoError(t, err)
	defer func() { assert.NoError(t, file.Close(ctx)) }()
	content, err := ioutil.ReadAll(ioctx.ToStdReader(ctx, file))
	require.NoError(t, err)
	return content
}
