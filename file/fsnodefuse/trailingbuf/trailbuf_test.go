package trailingbuf

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/grailbio/base/ioctx"
	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	const src = "0123456789"
	r := New(ioctx.FromStdReader(strings.NewReader(src)), 2)
	ctx := context.Background()

	// Initial read, larger than trail buf.
	b := make([]byte, 3)
	n, err := r.ReadAt(ctx, b, 0)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, "012", string(b))

	// Backwards by a little.
	b = make([]byte, 2)
	n, err = r.ReadAt(ctx, b, 1)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, "12", string(b))

	// Backwards by too much.
	b = make([]byte, 1)
	_, err = r.ReadAt(ctx, b, 0)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTooFarBehind)

	// Jump forward. Discards (not visible; exercising internal paths) because we skip off=3.
	b = make([]byte, 2)
	n, err = r.ReadAt(ctx, b, 4)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, "45", string(b))

	// Forwards, overlapping.
	b = make([]byte, 4)
	n, err = r.ReadAt(ctx, b, 4)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, "4567", string(b))

	// Jump again.
	b = make([]byte, 5)
	n, err = r.ReadAt(ctx, b, 9)
	if err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, 1, n)
	require.Equal(t, "9", string(b[:1]))

	// Make sure we can still read backwards after EOF.
	b = make([]byte, 1)
	n, err = r.ReadAt(ctx, b, 8)
	if err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, len(b), n)
	require.Equal(t, "8", string(b))
}

// TODO: Randomized, concurrent tests.
