package addfs

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/grailbio/base/file/fsnode"
	. "github.com/grailbio/base/file/fsnode/fsnodetesting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPerNodeFuncs(t *testing.T) {
	ctx := context.Background()
	root := func() Parent {
		return Parent{
			"dir0": Parent{},
			"dir1": Parent{
				"dir10": Parent{
					"a": []byte("content dir10/a"),
					"b": []byte("content dir10/b"),
				},
				"a": []byte("content dir1/a"),
				"b": []byte("content dir1/b"),
			},
		}
	}
	t.Run("basic", func(t *testing.T) {
		root := root()
		n := MakeT(t, "", root).(fsnode.Parent)
		n = ApplyPerNodeFuncs(n,
			NewPerNodeFunc(
				func(ctx context.Context, node fsnode.T) ([]fsnode.T, error) {
					switch n := node.(type) {
					case fsnode.Parent:
						iter := n.Children()
						defer func() { assert.NoError(t, iter.Close(ctx)) }()
						children, err := fsnode.IterateAll(ctx, iter)
						assert.NoError(t, err)
						var names []string
						for _, child := range children {
							names = append(names, child.Info().Name())
						}
						sort.Strings(names)
						return []fsnode.T{
							fsnode.ConstLeaf(fsnode.NewRegInfo("children names"), []byte(strings.Join(names, ","))),
						}, nil
					case fsnode.Leaf:
						return []fsnode.T{
							fsnode.ConstLeaf(fsnode.NewRegInfo("copy"), nil), // Will be overwritten.
						}, nil
					}
					require.Failf(t, "invalid node type", "node: %T", node)
					panic("unreachable")
				},
			),
			NewPerNodeFunc(
				func(ctx context.Context, node fsnode.T) ([]fsnode.T, error) {
					switch n := node.(type) {
					case fsnode.Parent:
						return nil, nil
					case fsnode.Leaf:
						return []fsnode.T{
							fsnode.ConstLeaf(fsnode.NewRegInfo("copy"), LeafReadAll(ctx, t, n)),
						}, nil
					}
					require.Failf(t, "invalid node type", "node: %T", node)
					panic("unreachable")
				},
			),
		)
		got := Walker{}.WalkContents(ctx, t, n)
		want := Parent{
			"...": Parent{
				"dir0": Parent{"children names": []byte("")},
				"dir1": Parent{"children names": []byte("a,b,dir10")},
			},
			"dir0": Parent{
				"...": Parent{},
			},
			"dir1": Parent{
				"...": Parent{
					"dir10": Parent{"children names": []byte("a,b")},
					"a":     Parent{"copy": []byte("content dir1/a")},
					"b":     Parent{"copy": []byte("content dir1/b")},
				},
				"dir10": Parent{
					"...": Parent{
						"a": Parent{"copy": []byte("content dir10/a")},
						"b": Parent{"copy": []byte("content dir10/b")},
					},
					"a": []byte("content dir10/a"),
					"b": []byte("content dir10/b"),
				},
				"a": []byte("content dir1/a"),
				"b": []byte("content dir1/b"),
			},
		}
		assert.Equal(t, want, got)
	})
	t.Run("lazy", func(t *testing.T) {
		root := root()
		n := MakeT(t, "", root).(fsnode.Parent)
		n = ApplyPerNodeFuncs(n, NewPerNodeFunc(
			func(_ context.Context, node fsnode.T) ([]fsnode.T, error) {
				return nil, fmt.Errorf("func was called: %q", node.Info().Name())
			},
		))
		got := Walker{
			IgnoredNames: map[string]struct{}{
				addsDirName: struct{}{},
			},
		}.WalkContents(ctx, t, n)
		assert.Equal(t, root, got)
	})
}
