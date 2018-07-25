package readmatchertest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"

	"github.com/grailbio/base/file/internal/kernel"
	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/base/must"
	"github.com/grailbio/base/traverse"
)

// Stress runs a stress test on rAt.
// If rAt is a readmatcher, consider setting parallelism below and above readmatcher.SoftMaxReaders
// to exercise those cases.
func Stress(want []byte, rAt ioctx.ReaderAt, parallelism int) {
	ctx := context.Background()

	size := len(want)
	// Only use rnd in the sequentially-executed task builders, not the parallelized actual reads.
	rnd := rand.New(rand.NewSource(1))

	const fuseReadSize = 1 << 17 // 128 KiB = FUSE max read size.
	sequentialBuilder := func() task {
		var t task
		start, limit := randInterval(rnd, size)
		for ; start < limit; start += fuseReadSize {
			limit := start + fuseReadSize
			if limit > size {
				limit = size
			}
			t = append(t, read{start, limit})
		}
		return t
	}
	taskBuilders := []func() task{
		// Read sequentially in FUSE-like chunks.
		sequentialBuilder,
		// Read some subset of the file, mostly sequentially, occasionally jumping.
		// The jumps reorder reads within the bounds of kernel.MaxReadAhead.
		// This is not quite the kernel readahead pattern because our reads are inherently
		// sequential, whereas kernel readahead parallelizes. But, assuming we know that there is
		// internal serialization somewhere, this at least simulates the variable ordering.
		func() task {
			// For simplicity, we choose to swap (or skip) adjacent pairs. Each item can only move 1
			// position, so the largest interval we can generate is changing a 1 read gap (adjacent)
			// into a 3 read gap.
			// If we allowed further, or second, swaps, we'd have to be careful about introducing
			// longer gaps. Maybe we'll do that later.
			must.True(kernel.MaxReadAhead >= 3*fuseReadSize)
			t := sequentialBuilder()
			for i := 0; i+1 < len(t); i += 2 {
				if rnd.Intn(2) == 0 {
					t[i], t[i+1] = t[i+1], t[i]
				}
			}
			return t
		},
		// Random reads covering some part of the data.
		func() task {
			t := sequentialBuilder()
			rnd.Shuffle(len(t), func(i, j int) { t[i], t[j] = t[j], t[i] })
			return t[:rnd.Intn(len(t))]
		},
	}
	tasks := make([]task, parallelism*10)
	for i := range tasks {
		tasks[i] = taskBuilders[rnd.Intn(len(taskBuilders))]()
	}
	err := traverse.T{Limit: parallelism}.Each(len(tasks), func(i int) (err error) {
		var dst []byte
		for _, t := range tasks[i] {
			readSize := t.limit - t.start
			if cap(dst) < readSize {
				dst = make([]byte, 2*readSize)
			}
			dst = dst[:readSize]
			n, err := rAt.ReadAt(ctx, dst, int64(t.start))
			if err == io.EOF {
				if n == readSize {
					err = nil
				} else {
					err = fmt.Errorf("early EOF: %d, %v", n, t)
				}
			}
			if err != nil {
				return err
			}
			if !bytes.Equal(want[t.start:t.limit], dst) {
				return fmt.Errorf("read mismatch: %v", t)
			}
		}
		return nil
	})
	must.Nil(err)
}

type (
	read struct{ start, limit int }
	task []read
)

// randInterval returns a subset of [0, size). Interval selection is biased so that a substantial
// number of returned intervals will touch 0 and/or size.
func randInterval(rnd *rand.Rand, size int) (start, limit int) {
	start = rnd.Intn(2*size) - size
	if start < 0 { // Around half will start at 0.
		start = 0
	}
	limit = start + rnd.Intn(2*(size-start+1))
	if limit > size { // And around half read till the end.
		limit = size
	}
	return
}
