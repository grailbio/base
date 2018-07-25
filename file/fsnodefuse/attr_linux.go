package fsnodefuse

import "github.com/hanwen/go-fuse/v2/fuse"

// blockSize is defined in the stat(2) man page:
//     The st_blocks field indicates the number of blocks allocated to the file, 512-byte units.  (This may be smaller than st_size/512 when the file has holes.)
const blockSize = 512

func setBlockSize(a *fuse.Attr, size uint32) {
	a.Blksize = size
}
