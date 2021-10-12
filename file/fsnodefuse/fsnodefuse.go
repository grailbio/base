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

	"github.com/grailbio/base/file/fsnode"
	"github.com/hanwen/go-fuse/v2/fs"
)

// NewRoot creates a FUSE inode whose contents are the given fsnode.T.
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
