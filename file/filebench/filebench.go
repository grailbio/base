package filebench

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/base/must"
	"github.com/grailbio/base/traverse"
)

// ReadSizes are the parameters for a benchmark run.
type ReadSizes struct {
	ChunkBytes       []int
	ContiguousChunks []int
	MaxReadBytes     int
	MaxReplicates    int
}

// ReplicateTargetBytes limits the number of replicates of a single benchmark condition.
const ReplicateTargetBytes int = 1e9

// DefaultReadSizes constructs ReadSizes with the default range of parameters.
func DefaultReadSizes() ReadSizes {
	return ReadSizes{
		ChunkBytes: []int{
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
		ContiguousChunks: []int{
			1,
			1 << 3,
			1 << 6,
			1 << 9,
		},
		MaxReadBytes:  1 << 32,
		MaxReplicates: 10,
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

type Prefix struct {
	Path string
	// MaxReadBytes optionally overrides ReadSizes.MaxReadBytes (only to become smaller).
	// Useful if one prefix (like FUSE) is slower than others.
	MaxReadBytes int
}

// RunAndPrint executes the benchmark cases and prints a human-readable summary to out.
// pathPrefixes is typically s3:// or a FUSE mount point. Results are reported for each one.
// pathSuffix* are at least one S3-relative path (like bucket/some/file.txt) to a large file to read
// during benchmarking. If there are multiple, reads are spread across them (not multiplied for each
// suffix). Caller may want to pass multiple to try to reduce throttling when several benchmark
// tasks are running in parallel (see Bigmachine.RunAndPrint).
func (r ReadSizes) RunAndPrint(
	ctx context.Context,
	out io.Writer,
	pathPrefixes []Prefix,
	pathSuffix0 string,
	pathSuffixes ...string,
) {
	minFileSize := r.MinFileSize()
	r.sort() // Make sure table is easy to read.

	pathSuffixes = append([]string{pathSuffix0}, pathSuffixes...)
	type fileOption struct {
		file.File
		Info file.Info
	}
	files := make([][]fileOption, len(pathPrefixes))
	for prefixIdx, prefix := range pathPrefixes {
		files[prefixIdx] = make([]fileOption, len(pathSuffixes))
		for suffixIdx, suffix := range pathSuffixes {
			f, err := file.Open(ctx, file.Join(prefix.Path, suffix))
			must.Nil(err)
			defer func() { must.Nil(f.Close(ctx)) }()
			o := &files[prefixIdx][suffixIdx]
			o.File = f

			o.Info, err = f.Stat(ctx)
			must.Nil(err)
			must.True(o.Info.Size() >= int64(minFileSize), "file too small", f.Name())
		}
	}

	type (
		condition struct {
			prefixIdx, chunkBytesIdx, contiguousChunksIdx int
			parallel                                      bool
		}
		result struct {
			totalBytes int
			totalTime  time.Duration
		}
	)
	var (
		tasks   []condition
		results = make([][][][]result, len(pathPrefixes))
	)
	for prefixIdx, prefix := range pathPrefixes {
		results[prefixIdx] = make([][][]result, len(r.ChunkBytes))
		for chunkBytesIdx, chunkBytes := range r.ChunkBytes {
			results[prefixIdx][chunkBytesIdx] = make([][]result, len(r.ContiguousChunks))
			for contiguousChunksIdx, contiguousChunks := range r.ContiguousChunks {
				results[prefixIdx][chunkBytesIdx][contiguousChunksIdx] = make([]result, 2)
				totalReadBytes := chunkBytes * contiguousChunks
				maxReadBytes := r.MaxReadBytes
				if 0 < prefix.MaxReadBytes && prefix.MaxReadBytes < maxReadBytes {
					maxReadBytes = prefix.MaxReadBytes
				}
				if totalReadBytes > maxReadBytes {
					continue
				}
				replicates := 1
				if totalReadBytes < ReplicateTargetBytes {
					replicates = (ReplicateTargetBytes - 1 + totalReadBytes) / totalReadBytes
					if replicates > r.MaxReplicates {
						replicates = r.MaxReplicates
					}
				}
				for _, parallel := range []bool{false, true} {
					for ri := 0; ri < replicates; ri++ {
						tasks = append(tasks, condition{
							prefixIdx:           prefixIdx,
							chunkBytesIdx:       chunkBytesIdx,
							contiguousChunksIdx: contiguousChunksIdx,
							parallel:            parallel,
						})
					}
				}
			}
		}
	}

	var (
		reproducibleRandom = rand.New(rand.NewSource(1))
		ephemeralRandom    = rand.New(rand.NewSource(time.Now().UnixNano()))
	)
	// While benchmarking is running, it's easy to compare the current task index from different
	// benchmarking machines to judge their relative progress.
	reproducibleRandom.Shuffle(len(tasks), func(i, j int) {
		tasks[i], tasks[j] = tasks[j], tasks[i]
	})

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
				prefix := pathPrefixes[c.prefixIdx]
				chunkBytes := r.ChunkBytes[c.chunkBytesIdx]
				contiguousChunks := r.ContiguousChunks[c.contiguousChunksIdx]
				log.Printf("done %d of %d tasks, current: %dB * %d on %s",
					taskIdx, len(tasks), chunkBytes, contiguousChunks, prefix.Path)
			case <-cancelled:
				break
			}
		}
	}()
	defer close(cancelled)

	dst := make([]byte, r.MaxReadBytes)
	for taskIdx, c := range tasks {
		atomic.StoreInt32(&currentTaskIdx, int32(taskIdx))

		chunkBytes := r.ChunkBytes[c.chunkBytesIdx]
		contiguousChunks := r.ContiguousChunks[c.contiguousChunksIdx]

		// Vary read locations non-reproducibly to try to spread load and avoid S3 throttling.
		// There's a tradeoff here: we're also likely introducing variance in benchmark results
		// if S3 read performance varies between objects and over time, which it probably does [1].
		// For now, empirically, it seems like throttling is the bigger problem, especially because
		// our benchmark runs are relatively brief (compared to large batch workloads) and thus
		// significantly affected by some throttling. We may revisit this in the future if a
		// different choice helps make the benchmark a better guide for optimization.
		//
		// [1] https://web.archive.org/web/20221220192142/https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
		f := files[c.prefixIdx][ephemeralRandom.Intn(len(pathSuffixes))]
		offset := ephemeralRandom.Int63n(f.Info.Size() - int64(chunkBytes*contiguousChunks) + 1)

		parIdx := 0
		start := time.Now()
		func() {
			var (
				traverser traverse.T
				chunks    = make([]struct {
					r   io.Reader
					dst []byte
				}, contiguousChunks)
			)
			if c.parallel {
				parIdx = 1
				for i := range chunks {
					chunkOffset := i * chunkBytes
					rc := f.OffsetReader(offset + int64(chunkOffset))
					defer func() { must.Nil(rc.Close(ctx)) }()
					chunks[i].r = ioctx.ToStdReader(ctx, rc)
					chunks[i].dst = dst[chunkOffset : chunkOffset+chunkBytes]
				}
			} else {
				traverser.Limit = 1
				rc := ioctx.ToStdReadCloser(ctx, f.OffsetReader(offset))
				defer func() { must.Nil(rc.Close()) }()
				for i := range chunks {
					chunks[i].r = rc
					chunks[i].dst = dst[:chunkBytes]
				}
			}
			_ = traverser.Each(contiguousChunks, func(i int) error {
				n, err := io.ReadFull(chunks[i].r, chunks[i].dst)
				must.Nil(err)
				must.True(n == chunkBytes)
				return nil
			})
		}()
		elapsed := time.Since(start)

		results[c.prefixIdx][c.chunkBytesIdx][c.contiguousChunksIdx][parIdx].totalBytes += chunkBytes * contiguousChunks
		results[c.prefixIdx][c.chunkBytesIdx][c.contiguousChunksIdx][parIdx].totalTime += elapsed
	}

	tw := tabwriter.NewWriter(out, 0, 4, 4, ' ', 0)
	mustPrintf := func(format string, args ...interface{}) {
		_, err := fmt.Fprintf(tw, format, args...)
		must.Nil(err)
	}
	mustPrintf("\t")
	for _, prefix := range pathPrefixes {
		mustPrintf("%s%s", prefix.Path, strings.Repeat("\t", 2*len(r.ContiguousChunks)))
	}
	mustPrintf("\n")
	for range files {
		for _, parLabel := range []string{"", "p"} {
			for _, contiguousChunks := range r.ContiguousChunks {
				mustPrintf("\t%s%d", parLabel, contiguousChunks)
			}
		}
	}
	mustPrintf("\n")
	for chunkBytesIdx, chunkBytes := range r.ChunkBytes {
		mustPrintf("%d", chunkBytes/(1<<20))
		for prefixIdx := range files {
			for _, parIdx := range []int{0, 1} {
				for contiguousChunksIdx := range r.ContiguousChunks {
					s := results[prefixIdx][chunkBytesIdx][contiguousChunksIdx][parIdx]
					mustPrintf("\t")
					if s.totalTime > 0 {
						mibs := float64(s.totalBytes) / s.totalTime.Seconds() / float64(1<<20)
						mustPrintf("%.f", mibs)
					}
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
