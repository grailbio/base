package fsnodetesting

import (
	"testing"

	"github.com/grailbio/base/file/fsnode"
	"github.com/stretchr/testify/require"
)

// MakeT is the inverse of (Walker).WalkContents.
// It keeps no references to node and its children after it returns.
func MakeT(t testing.TB, name string, node T) fsnode.T {
	switch n := node.(type) {
	case Parent:
		var children []fsnode.T
		for childName, child := range n {
			children = append(children, MakeT(t, childName, child))
		}
		return fsnode.NewParent(fsnode.NewDirInfo(name), fsnode.ConstChildren(children...))
	case Leaf:
		return fsnode.ConstLeaf(fsnode.NewRegInfo(name), append([]byte{}, n...))
	}
	require.Failf(t, "invalid node type", "node: %T", node)
	panic("unreachable")
}
