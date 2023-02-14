// fsnodefuse implements github.com/hanwen/go-fuse/v2/fs for fsnode.T.
// It's a work-in-progress. No correctness or stability is guaranteed. Or even suggested.
//
// fsnode.Parent naturally becomes a directory. fsnode.Leaf becomes a file. Support for FUSE
// operations on that file depends on what Leaf.Open returns. If that fsctx.File is also
//   spliceio.ReaderAt:
//     FUSE file supports concurrent, random reads and uses splices to reduce
//     userspace <-> kernelspace memory copying.
//   ioctx.ReaderAt:
//     FUSE file supports concurrent, random reads.
//   Otherwise:
//     FUSE file supports in-order, contiguous reads only. That is, each read must
//     start where the previous one ended.	At fsctx.File EOF, file size is recorded
//     and then overrides what fsctx.File.Stat() reports for future getattr calls,
//     so users can see they're done reading.
//     TODO: Decide if there's a better place for this feature.
package fsnodefuse

import (
	"fmt"
	"runtime"

	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/file/internal/kernel"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// NewRoot creates a FUSE inode whose contents are the given fsnode.T.
// Note that this inode must be mounted with options from ConfigureRequiredMountOptions.
func NewRoot(node fsnode.T) fs.InodeEmbedder {
	switch n := node.(type) {
	case fsnode.Parent:
		return &dirInode{n: n}
	case fsnode.Leaf:
		// TODO(josh): Test this path.
		return &regInode{n: n}
	}
	panic(fmt.Sprintf("unrecognized fsnode type: %T, %[1]v", node))
}

// ConfigureRequiredMountOptions sets values in opts to be compatible with fsnodefuse's
// implementation. Users of NewRoot must use these options, and they should call this last,
// to make sure the required options take effect.
func ConfigureRequiredMountOptions(opts *fuse.MountOptions) {
	opts.MaxReadAhead = kernel.MaxReadAhead
}

// ConfigureDefaultMountOptions provides defaults that callers may want to start with, for performance.
func ConfigureDefaultMountOptions(opts *fuse.MountOptions) {
	// Increase MaxBackground from its default value (12) to improve S3 read performance.
	//
	// Empirically, while reading a 30 GiB files in chunks in parallel, the number of concurrent
	// reads processed by our FUSE server [1] was ~12 with the default, corresponding to poor
	// network utilization (only 500 Mb/s on m5d.4x in EC2); it rises to ~120 after, and network
	// read bandwidth rises to >7 Gb/s, close to the speed of reading directly from S3 with
	// this machine (~9 Gb/s).
	//
	// libfuse documentation [2] suggests that this limits the number of kernel readahead
	// requests, so raising the limit may allow kernel readahead for every chunk, which could
	// plausibly explain the performance benefit. (There's also mention of large direct I/O
	// requests from userspace; josh@ did not think his Go test program was using direct I/O for
	// this benchmark, but maybe he just didn't know).
	//
	// This particular value is a somewhat-informed guess. We'd like it to be high enough to
	// admit all the parallelism that applications may profitably want. EC2 instances generally
	// have <1 Gb/s network bandwidth per CPU (m5n.24x is around that, and non-'n' types have
	// several times less), and S3 connections are limited to ~700 Mb/s [3], so just a couple of
	// read chunks per CPU are sufficient to be I/O-bound for large objects. Many smaller object
	// reads tend to not reach maximum bandwidth, so applications may increase parallelism,
	// so we set our limit several times higher.
	// TODO: Run more benchmarks (like github.com/grailbio/base/file/filebench) and tune.
	//
	// [1] As measured by simple logging: https://gitlab.com/grailbio/grail/-/merge_requests/8292/diffs?commit_id=7681acfcac836b92eaca60eb567245b32b81ec50
	// [2] https://web.archive.org/web/20220815053939/https://libfuse.github.io/doxygen/structfuse__conn__info.html#a5f9e695735727343448ae1e1a86dfa03
	// [3] 85-90 MB/s: https://web.archive.org/web/20220325121400/https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html#optimizing-performance-parallelization
	opts.MaxBackground = 16 * runtime.NumCPU()

	// We don't use extended attributes so we can skip these requests to improve performance.
	opts.DisableXAttrs = true
}
