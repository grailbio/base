package morebufio

import (
	"context"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/base/traverse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReaderAt(t *testing.T) {
	const want = "abcdefghijklmnopqrstuvwxyz"
	ctx := context.Background()
	rawReaderAt := ioctx.FromStdReaderAt(strings.NewReader(want))

	t.Run("sequential", func(t *testing.T) {
		bufAt := NewReaderAtSize(rawReaderAt, 5)
		got := make([]byte, 0, len(want))
		for len(got) < len(want) {
			n, err := bufAt.ReadAt(ctx, got[len(got):cap(got)], int64(len(got)))
			got = got[:len(got)+n]
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
		assert.Equal(t, want, string(got))
	})

	t.Run("random", func(t *testing.T) {
		rnd := rand.New(rand.NewSource(1))
		for _, parallelism := range []int{1, len(want) / 2} {
			t.Run(strconv.Itoa(parallelism), func(t *testing.T) {
				bufAt := NewReaderAtSize(rawReaderAt, 5)
				got := make([]byte, len(want))
				perm := rnd.Perm(len(want) / 2)
				_ = traverse.T{Limit: parallelism}.Each(len(perm), func(permIdx int) error {
					i := perm[permIdx]
					start := i * 2
					limit := start + 2
					if limit > len(got) {
						limit -= 1
					}
					n, err := bufAt.ReadAt(ctx, got[start:limit], int64(start))
					assert.Equal(t, limit-start, n)
					if limit < len(got) || err != io.EOF {
						require.NoError(t, err)
					}
					return nil
				})
				assert.Equal(t, want, string(got))
			})
		}
	})
}
