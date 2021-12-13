package morebufio

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/grailbio/base/ioctx"
	"github.com/stretchr/testify/require"
)

const digits = "0123456789"

func TestPeekBack(t *testing.T) {
	ctx := context.Background()
	r := NewPeekBackReader(ioctx.FromStdReader(strings.NewReader(digits)), 4)

	// Initial read, smaller than peek buf.
	b := make([]byte, 2)
	n, err := r.Read(ctx, b)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, "01", string(b))
	require.Equal(t, "01", string(r.PeekBack()))

	// Read enough to shift buf.
	b = make([]byte, 3)
	n, err = r.Read(ctx, b)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, "234", string(b))
	require.Equal(t, "1234", string(r.PeekBack()))

	// Read nothing.
	b = nil
	n, err = r.Read(ctx, b)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, "1234", string(r.PeekBack()))

	// Read past EOF.
	b = make([]byte, 8)
	n, err = r.Read(ctx, b)
	if err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, 5, n)
	require.Equal(t, "56789", string(b[:n]))
	require.Equal(t, "6789", string(r.PeekBack()))

	n, err = r.Read(ctx, b)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 0, n)
	require.Equal(t, "6789", string(r.PeekBack()))
}

func TestPeekBackLargeInitial(t *testing.T) {
	ctx := context.Background()
	r := NewPeekBackReader(ioctx.FromStdReader(strings.NewReader(digits)), 3)

	// Initial read, larger than peek buf.
	b := make([]byte, 6)
	n, err := r.Read(ctx, b)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, "012345", string(b))
	require.Equal(t, "345", string(r.PeekBack()))

	// Shift.
	b = make([]byte, 1)
	n, err = r.Read(ctx, b)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, "6", string(b))
	require.Equal(t, "456", string(r.PeekBack()))
}

func TestPeekBackNeverFull(t *testing.T) {
	ctx := context.Background()
	r := NewPeekBackReader(ioctx.FromStdReader(strings.NewReader(digits)), 20)

	b := make([]byte, 6)
	n, err := r.Read(ctx, b)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, "012345", string(b))
	require.Equal(t, "012345", string(r.PeekBack()))

	b = make([]byte, 20)
	n, err = r.Read(ctx, b)
	if err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, 4, n)
	require.Equal(t, "6789", string(b[:n]))
	require.Equal(t, "0123456789", string(r.PeekBack()))

	n, err = r.Read(ctx, b)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 0, n)
	require.Equal(t, "0123456789", string(r.PeekBack()))
}

func TestPeekBackZero(t *testing.T) {
	ctx := context.Background()
	r := NewPeekBackReader(ioctx.FromStdReader(strings.NewReader(digits)), 0)

	b := make([]byte, 6)
	n, err := r.Read(ctx, b)
	require.NoError(t, err)
	require.Equal(t, len(b), n)
	require.Equal(t, "012345", string(b))
	require.Equal(t, "", string(r.PeekBack()))

	b = make([]byte, 20)
	n, err = r.Read(ctx, b)
	if err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, 4, n)
	require.Equal(t, "6789", string(b[:n]))
	require.Equal(t, "", string(r.PeekBack()))

	n, err = r.Read(ctx, b)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 0, n)
	require.Equal(t, "", string(r.PeekBack()))
}

// TODO: Randomized/fuzz tests.
