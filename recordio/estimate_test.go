package recordio_test

import (
	"testing"

	"github.com/grailbio/base/recordio"
	"github.com/grailbio/testutil/assert"
)

const (
	KiB = int64(1024)
	MiB = int64(1024 * 1024)
)

func TestRequiredSpaceUpperBound(t *testing.T) {
	for _, test := range []struct {
		itemSizes        []int64
		recordSize       int64
		expectedReqSpace int64
	}{
		// internal.ChunkSize == 32KiB

		{ // recordSize < chunkSize
			[]int64{1 * KiB, 1 * KiB, 1 * KiB},
			1 * KiB,
			6 * 32 * KiB,
		},
		{ // recordSize < chunkSize
			[]int64{1 * KiB, 1 * KiB, 1 * KiB},
			2 * KiB,
			4 * 32 * KiB,
		},
		{ // chunkSize < recordSize
			[]int64{5 * MiB, 2 * KiB, 12 * MiB, 3 * MiB},
			4 * MiB,
			776 * 32 * KiB,
		},
		{ // recordSize == chunkSize
			[]int64{35 * KiB, 9 * KiB, 1 * MiB, 20 * KiB},
			32 * KiB,
			72 * 32 * KiB,
		},
		{ // sizes where no-padding of chunks is required
			[]int64{32736, 32736, 32732 + 32740 + 32736},
			32*KiB - 32,
			8 * 32 * KiB,
		},
	} {
		req := recordio.RequiredSpaceUpperBound(test.itemSizes, test.recordSize)
		sum := int64(0)
		for _, v := range test.itemSizes {
			sum += v
		}
		assert.GT(t, req, sum)
		assert.EQ(t, req, test.expectedReqSpace)
	}
}
