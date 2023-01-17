package filebench

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sort"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/must"
)

// ReadSizes are the parameters for a benchmark run.
type ReadSizes struct {
	ChunkBytes       []int
	ContiguousChunks []int
	MaxReadBytes     int
}

// DefaultReadSizes constructs ReadSizes with the default range of parameters.
func DefaultReadSizes() ReadSizes {
	return ReadSizes{
		[]int{
			1 << 10,
			1 << 20,
			1 << 23,
			1 << 24,
			1 << 25,
			1 << 27,
			1 << 29,
			1 << 30,
			1 << 32,
		},
		[]int{
			1,
			1 << 3,
			1 << 6,
			1 << 9,
		},
		1 << 32,
	}
}

func (r ReadSizes) MinFileSize() int {
	size := maxInts(r.ChunkBytes) * maxInts(r.ContiguousChunks)
	if size < r.MaxReadBytes {
		return size
	}
	return r.MaxReadBytes
}

func (r ReadSizes) sort() {
	must.True(len(r.ChunkBytes) > 0)
	must.True(len(r.ContiguousChunks) > 0)
	sort.Ints(r.ChunkBytes)
	sort.Ints(r.ContiguousChunks)
}

// RunAndPrint executes the benchmark cases and prints a human-readable summary to w.
func (r ReadSizes) RunAndPrint(ctx context.Context, out io.Writer, paths ...string) {
	minFileSize := r.MinFileSize()
	r.sort() // Make sure table is easy to read.

	readers := make([]struct {
		io.ReadSeeker
		Info file.Info
	}, len(paths))
	for i, path := range paths {
		f, err := file.Open(ctx, path)
		must.Nil(err)
		defer func() { must.Nil(f.Close(ctx)) }()
		readers[i].ReadSeeker = f.Reader(ctx)

		readers[i].Info, err = f.Stat(ctx)
		must.Nil(err)
		must.True(readers[i].Info.Size() >= int64(minFileSize), "file too small")
	}

	rnd := rand.New(rand.NewSource(1))

	type (
		condition struct{ pathIdx, chunkBytesIdx, contiguousChunksIdx int }
		result    struct {
			totalBytes int
			totalTime  time.Duration
		}
	)
	var (
		tasks   []condition
		results = make([][][]result, len(paths))
	)
	for pathIdx := range paths {
		results[pathIdx] = make([][]result, len(r.ChunkBytes))
		for chunkBytesIdx, chunkBytes := range r.ChunkBytes {
			results[pathIdx][chunkBytesIdx] = make([]result, len(r.ContiguousChunks))
			for contiguousChunksIdx, contiguousChunks := range r.ContiguousChunks {
				totalReadBytes := chunkBytes * contiguousChunks
				if totalReadBytes > r.MaxReadBytes {
					continue
				}
				replicates := 1
				const targetReadSize = 1e9
				if totalReadBytes < targetReadSize {
					replicates = (targetReadSize - 1 + totalReadBytes) / totalReadBytes
					if replicates > 20 {
						replicates = 20
					}
				}
				for ri := 0; ri < replicates; ri++ {
					tasks = append(tasks, condition{pathIdx, chunkBytesIdx, contiguousChunksIdx})
				}
			}
		}
	}
	rnd.Shuffle(len(tasks), func(i, j int) {
		tasks[i], tasks[j] = tasks[j], tasks[i]
	})
	dst := make([]byte, r.ChunkBytes[len(r.ChunkBytes)-1])

	var (
		currentTaskIdx int32
		cancelled      = make(chan struct{})
	)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				taskIdx := atomic.LoadInt32(&currentTaskIdx)
				c := tasks[taskIdx]
				chunkBytes := r.ChunkBytes[c.chunkBytesIdx]
				contiguousChunks := r.ContiguousChunks[c.contiguousChunksIdx]
				log.Printf("done %d of %d tasks, current: %dB * %d",
					taskIdx, len(tasks), chunkBytes, contiguousChunks)
			case <-cancelled:
				break
			}
		}
	}()
	defer close(cancelled)

	for taskIdx, c := range tasks {
		atomic.StoreInt32(&currentTaskIdx, int32(taskIdx))

		reader := readers[c.pathIdx]
		chunkBytes := r.ChunkBytes[c.chunkBytesIdx]
		contiguousChunks := r.ContiguousChunks[c.contiguousChunksIdx]
		offset := rnd.Int63n(reader.Info.Size() - int64(chunkBytes*contiguousChunks))
		_, err := reader.Seek(offset, io.SeekStart)
		must.Nil(err)

		start := time.Now()
		var totalReadBytes int
		for i := 0; i < contiguousChunks; i++ {
			n, err := io.ReadFull(reader, dst[:chunkBytes])
			totalReadBytes += n
			must.Nil(err)
		}
		elapsed := time.Since(start)

		results[c.pathIdx][c.chunkBytesIdx][c.contiguousChunksIdx].totalBytes += totalReadBytes
		results[c.pathIdx][c.chunkBytesIdx][c.contiguousChunksIdx].totalTime += elapsed
	}

	tw := tabwriter.NewWriter(out, 0, 4, 4, ' ', 0)
	mustPrintf := func(format string, args ...interface{}) {
		_, err := fmt.Fprintf(tw, format, args...)
		must.Nil(err)
	}
	for range paths {
		for _, contiguousChunks := range r.ContiguousChunks {
			mustPrintf("\t%d", contiguousChunks)
		}
	}
	mustPrintf("\n")
	for chunkBytesIdx, chunkBytes := range r.ChunkBytes {
		mustPrintf("%d", chunkBytes/(1<<20))
		for pathIdx := range paths {
			for contiguousChunksIdx := range r.ContiguousChunks {
				s := results[pathIdx][chunkBytesIdx][contiguousChunksIdx]
				mustPrintf("\t")
				if s.totalTime > 0 {
					mibs := float64(s.totalBytes) / s.totalTime.Seconds() / float64(1<<20)
					mustPrintf("%.f", mibs)
				}
			}
		}
		mustPrintf("\n")
	}
	must.Nil(tw.Flush())
}

func maxInts(ints []int) int {
	if len(ints) == 0 {
		return 0 // OK for our purposes.
	}
	max := ints[0]
	for _, i := range ints[1:] {
		if i > max {
			max = i
		}
	}
	return max
}
