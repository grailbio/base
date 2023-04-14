package addfs

import (
	"context"
	"fmt"
	"time"

	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
)

// TODO: Implement PerSubtreeFunc.
// A PerNodeFunc is applied independently to each node in an entire directory tree. It may be
// useful to define funcs that are contextual. For example if an fsnode.Parent called base/ has a
// child called .git, we may want to define git-repository-aware views for each descendent node,
// like base/file/addfs/.../per_subtree.go/git/log.txt containing history.

type (
	PerSubtreeFunc interface {
		Apply(context.Context, fsnode.T) (_ PerSubtreeFunc, adds []fsnode.T, _ error)
	}
	perSubtreeFunc func(context.Context, fsnode.T) (_ PerSubtreeFunc, adds []fsnode.T, _ error)
)

const addsDirName = "..."

type perSubtreeImpl struct {
	fsnode.Parent
	fns  []PerSubtreeFunc
	adds fsnode.Parent
}

// TODO
func ApplyPerSubtreeFuncs(original fsnode.Parent, fns ...PerSubtreeFunc) fsnode.Parent {
	fns = append([]PerSubtreeFunc{}, fns...)
	adds := perSubtreeAdds{
		fsnode.CopyFileInfo(original).WithName(addsDirName),
		original, fns}
	return &perSubtreeImpl{original, fns, &adds}
}

func (n *perSubtreeImpl) CacheableFor() time.Duration { return fsnode.CacheableFor(n.Parent) }
func (n *perSubtreeImpl) Child(ctx context.Context, name string) (fsnode.T, error) {
	if name == addsDirName {
		return n.adds, nil
	}
	child, err := n.Parent.Child(ctx, name)
	if err != nil {
		return nil, err
	}
	return perSubtreeRecurse(child, n.fns), nil
}
func (n *perSubtreeImpl) Children() fsnode.Iterator {
	return fsnode.NewConcatIterator(
		// TODO: Consider omitting .../ if the directory has no other children.
		fsnode.NewIterator(n.adds),
		// TODO: Filter out any conflicting ... to be consistent with Child.
		fsnode.MapIterator(n.Parent.Children(), func(_ context.Context, child fsnode.T) (fsnode.T, error) {
			return perSubtreeRecurse(child, n.fns), nil
		}),
	)
}

type perSubtreeAdds struct {
	fsnode.FileInfo
	original fsnode.Parent
	fns      []PerSubtreeFunc
}

var (
	_ fsnode.Parent    = (*perSubtreeAdds)(nil)
	_ fsnode.Cacheable = (*perSubtreeAdds)(nil)
)

func (n *perSubtreeAdds) Child(ctx context.Context, name string) (fsnode.T, error) {
	child, err := n.original.Child(ctx, name)
	if err != nil {
		return nil, err
	}
	return n.newAddsForChild(child), nil
}
func (n *perSubtreeAdds) Children() fsnode.Iterator {
	// TODO: Filter out any conflicting ... to be consistent with Child.
	return fsnode.MapIterator(n.original.Children(), func(_ context.Context, child fsnode.T) (fsnode.T, error) {
		return n.newAddsForChild(child), nil
	})
}
func (n *perSubtreeAdds) FSNodeT() {}

func (n *perSubtreeAdds) newAddsForChild(original fsnode.T) fsnode.Parent {
	return fsnode.NewParent(
		fsnode.NewDirInfo(original.Name()).
			WithModTime(original.ModTime()).
			// Derived directory must be executable to be usable, even if original file wasn't.
			WithModePerm(original.Mode().Perm()|0111).
			WithCacheableFor(fsnode.CacheableFor(original)),
		fsnode.FuncChildren(func(ctx context.Context) ([]fsnode.T, error) {
			var (
				fnsRecurses = make([]PerSubtreeFunc, len(n.fns))
				fnsAdds     = make([][]fsnode.T, len(n.fns))
				adds        = make(map[string]fsnode.T)
			)
			err := traverse.Each(len(n.fns), func(i int) (err error) {
				fnsRecurses[i], fnsAdds[i], err = n.fns[i].Apply(ctx, original)
				if err != nil {
					return fmt.Errorf("addfs: error running func %v: %w", n.fns[i], err)
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			for _, fnAdds := range fnsAdds {
				for _, add := range fnAdds {
					if _, exists := adds[add.Name()]; exists {
						// TODO: Consider returning an error here. Or merging the added trees?
						log.Error.Printf("addfs %s: conflict for added name: %s", original.Name(), add.Name())
					}
					adds[add.Name()] = add
				}
			}
			wrapped := make([]fsnode.T, 0, len(adds))
			for _, add := range adds {
				wrapped = append(wrapped, perSubtreeRecurse(add, fnsRecurses))
			}
			return wrapped, nil
		}),
	)
}

func perSubtreeRecurse(node fsnode.T, fns []PerSubtreeFunc) fsnode.T {
	parent, ok := node.(fsnode.Parent)
	if !ok {
		return node
	}
	return ApplyPerSubtreeFuncs(parent, fns...)
}
