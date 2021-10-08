package fsnode

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSliceAll(t *testing.T) {
	ctx := context.Background()
	nodes := []mockLeaf{{id: 0}, {id: 1}, {id: 2}}
	got, err := IterateAll(ctx, NewIterator(nodes[0], nodes[1], nodes[2]))
	require.NoError(t, err)
	assert.Equal(t, []T{nodes[0], nodes[1], nodes[2]}, got)
}

func TestSliceFull(t *testing.T) {
	ctx := context.Background()
	nodes := []mockLeaf{{id: 0}, {id: 1}, {id: 2}}
	dst := make([]T, 2)

	iter := NewIterator(nodes[0], nodes[1], nodes[2])
	got, err := IterateFull(ctx, iter, dst)
	require.NoError(t, err)
	assert.Equal(t, 2, got)
	assert.Equal(t, []T{nodes[0], nodes[1]}, dst)

	got, err = IterateFull(ctx, iter, dst)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 1, got)
	assert.Equal(t, []T{nodes[2]}, dst[:1])
}

func TestConcatAll(t *testing.T) {
	ctx := context.Background()
	nodes := []mockLeaf{{id: 0}, {id: 1}, {id: 2}, {id: 3}}
	iter := NewConcatIterator(
		NewIterator(),
		NewIterator(nodes[0]),
		NewIterator(),
		NewIterator(),
		NewIterator(nodes[1], nodes[2], nodes[3]),
		NewIterator(),
	)
	got, err := IterateAll(ctx, iter)
	require.NoError(t, err)
	assert.Equal(t, []T{nodes[0], nodes[1], nodes[2], nodes[3]}, got)
}

func TestConcatFull(t *testing.T) {
	ctx := context.Background()
	nodes := []mockLeaf{{id: 0}, {id: 1}, {id: 2}, {id: 3}}
	iter := NewConcatIterator(
		NewIterator(),
		NewIterator(nodes[0]),
		NewIterator(),
		NewIterator(),
		NewIterator(nodes[1], nodes[2], nodes[3]),
		NewIterator(),
	)

	var dst []T
	got, err := IterateFull(ctx, iter, dst)
	require.NoError(t, err)
	assert.Equal(t, 0, got)

	dst = make([]T, 3)
	got, err = IterateFull(ctx, iter, dst[:2])
	require.NoError(t, err)
	assert.Equal(t, 2, got)
	assert.Equal(t, []T{nodes[0], nodes[1]}, dst[:2])

	got, err = IterateFull(ctx, iter, dst[:1])
	require.NoError(t, err)
	assert.Equal(t, 1, got)
	assert.Equal(t, []T{nodes[2]}, dst[:1])

	got, err = IterateFull(ctx, iter, dst)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 1, got)
	assert.Equal(t, []T{nodes[3]}, dst[:1])
}

type mockLeaf struct {
	Leaf
	id int
}
