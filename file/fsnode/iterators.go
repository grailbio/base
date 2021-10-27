package fsnode

import (
	"context"
	"io"
	"os"
)

type sliceIterator struct {
	remaining []T
	closed    bool
}

var _ Iterator = (*sliceIterator)(nil)

// NewIterator returns an iterator that yields the given nodes.
func NewIterator(nodes ...T) Iterator {
	// Copy the slice because we'll mutate to nil later.
	nodes = append([]T{}, nodes...)
	return &sliceIterator{remaining: nodes}
}

func (it *sliceIterator) Next(ctx context.Context) (T, error) {
	if it.closed {
		return nil, os.ErrClosed
	}
	if len(it.remaining) == 0 {
		return nil, io.EOF
	}
	next := it.remaining[0]
	it.remaining[0] = nil // TODO: Is this necessary to allow GC?
	it.remaining = it.remaining[1:]
	return next, nil
}

func (it *sliceIterator) Close(context.Context) error {
	it.closed = true
	it.remaining = nil
	return nil
}

type lazyIterator struct {
	make     func(context.Context) ([]T, error)
	fetched  bool
	delegate Iterator
}

var _ Iterator = (*lazyIterator)(nil)

// NewLazyIterator uses the given make function upon the first call to Next to
// make the nodes that it yields.
func NewLazyIterator(make func(context.Context) ([]T, error)) Iterator {
	return &lazyIterator{make: make}
}

func (it *lazyIterator) Next(ctx context.Context) (T, error) {
	if err := it.ensureFetched(ctx); err != nil {
		return nil, err
	}
	return it.delegate.Next(ctx)
}

func (it *lazyIterator) Close(ctx context.Context) error {
	if it.delegate == nil {
		return nil
	}
	err := it.delegate.Close(ctx)
	it.delegate = nil
	return err
}

func (it *lazyIterator) ensureFetched(ctx context.Context) error {
	if it.fetched {
		if it.delegate == nil {
			return os.ErrClosed
		}
		return nil
	}
	nodes, err := it.make(ctx)
	if err != nil {
		return err
	}
	it.delegate = NewIterator(nodes...)
	it.fetched = true
	return nil
}

type concatIterator struct {
	iters  []Iterator
	closed bool
}

var _ Iterator = (*concatIterator)(nil)

// NewConcatIterator returns the elements of the given iterators in order, reading each until EOF.
// Manages calling Close on constituents (as it goes along and upon its own Close).
func NewConcatIterator(iterators ...Iterator) Iterator {
	return &concatIterator{iters: append([]Iterator{}, iterators...)}
}

func (it *concatIterator) Next(ctx context.Context) (T, error) {
	if it.closed {
		return nil, os.ErrClosed
	}
	for {
		if len(it.iters) == 0 {
			return nil, io.EOF
		}
		next, err := it.iters[0].Next(ctx)
		if err == io.EOF {
			err = nil
			if next != nil {
				err = iteratorEOFError(it.iters[0])
			}
			if closeErr := it.iters[0].Close(ctx); closeErr != nil && err == nil {
				err = closeErr
			}
			it.iters[0] = nil // TODO: Is this actually necessary to allow GC?
			it.iters = it.iters[1:]
			if err != nil {
				return nil, err
			}
			continue
		}
		return next, err
	}
}

// Close attempts to Close remaining constituent iterators. Returns the first constituent Close
// error (but attempts to close the rest anyway).
func (it *concatIterator) Close(ctx context.Context) error {
	it.closed = true
	var err error
	for _, iter := range it.iters {
		if err2 := iter.Close(ctx); err2 != nil && err == nil {
			err = err2
		}
	}
	it.iters = nil
	return err
}

type mapIterator struct {
	iter Iterator
	fn   func(context.Context, T) (T, error)
}

// MapIterator returns an Iterator that applies fn to each T yielded by iter.
func MapIterator(iter Iterator, fn func(context.Context, T) (T, error)) Iterator {
	return mapIterator{iter, fn}
}
func (it mapIterator) Next(ctx context.Context) (T, error) {
	if it.fn == nil {
		return nil, os.ErrClosed
	}
	node, err := it.iter.Next(ctx)
	if err == nil && it.fn != nil {
		node, err = it.fn(ctx, node)
	}
	return node, err
}
func (it mapIterator) Close(ctx context.Context) error {
	it.fn = nil
	return it.iter.Close(ctx)
}
