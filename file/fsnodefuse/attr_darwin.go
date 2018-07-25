package fsnodefuse

import "github.com/hanwen/go-fuse/v2/fuse"

// blockSize is defined in the stat(2) man page:
//     st_blocks      The actual number of blocks allocated for the file in 512-byte units.  As short symbolic links are stored in the inode, this number may be zero.
const blockSize = 512

func setBlockSize(*fuse.Attr, uint32) {
	// a.Blksize not present on darwin.
	// TODO: Implement statfs for darwin to pass iosize.
}
