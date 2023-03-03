package readmatchertest

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRandInterval(t *testing.T) {
	const (
		size   = 10000
		trials = 10000
	)
	var (
		rnd                  = rand.New(rand.NewSource(1))
		touchZero, touchSize int
	)
	for i := 0; i < trials; i++ {
		start, limit := randInterval(rnd, size)
		require.GreaterOrEqual(t, start, 0)
		require.LessOrEqual(t, limit, size)
		if start == 0 {
			touchZero++
		}
		if limit == size {
			touchSize++
		}
	}
	// 10% is a very loose constraint. We could be more precise, but we don't care too much.
	assert.Greater(t, touchZero, trials/10)
	assert.Greater(t, touchSize, trials/10)
}
