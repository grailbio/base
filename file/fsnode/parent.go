package fsnode

import (
	"context"
	"os"

	"github.com/grailbio/base/log"
)

// NewParent returns a Parent whose children are defined by gen. gen.GenerateChildren is called on
// every Parent query (including Child, which returns one result). Implementers should cache
// internally if necessary.
func NewParent(info FileInfo, gen ChildrenGenerator) Parent {
	if !info.IsDir() {
		log.Panicf("FileInfo has file mode, require directory: %#v", info)
	}
	return parentImpl{info, gen}
}

type (
	// ChildrenGenerator generates child nodes.
	ChildrenGenerator interface {
		GenerateChildren(context.Context) ([]T, error)
	}
	childrenGenFunc  func(context.Context) ([]T, error)
	childrenGenConst []T
)

// FuncChildren constructs a ChildrenGenerator that simply invokes fn, for convenience.
func FuncChildren(fn func(context.Context) ([]T, error)) ChildrenGenerator {
	return childrenGenFunc(fn)
}

func (fn childrenGenFunc) GenerateChildren(ctx context.Context) ([]T, error) { return fn(ctx) }

// ConstChildren constructs a ChildrenGenerator that always returns the given children.
func ConstChildren(children ...T) ChildrenGenerator {
	children = append([]T{}, children...)
	return childrenGenConst(children)
}

func (c childrenGenConst) GenerateChildren(ctx context.Context) ([]T, error) { return c, nil }

type parentImpl struct {
	FileInfo
	gen ChildrenGenerator
}

func (n parentImpl) Child(ctx context.Context, name string) (T, error) {
	children, err := n.gen.GenerateChildren(ctx)
	if err != nil {
		return nil, err
	}
	for _, child := range children {
		if child.Name() == name {
			return child, nil
		}
	}
	return nil, os.ErrNotExist
}

func (n parentImpl) Children() Iterator {
	return NewLazyIterator(n.gen.GenerateChildren)
}

func (l parentImpl) FSNodeT() {}
