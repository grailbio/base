package fsnodefuse

import (
	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/log"
	"github.com/hanwen/go-fuse/v2/fs"
)

// setFSNode updates inode to be backed by fsNode.  The caller must ensure that
// inode and fsNode are compatible:
//  *dirInode <-> fsnode.Parent
//  *regInode <-> fsnode.Leaf
func setFSNode(inode *fs.Inode, fsNode fsnode.T) {
	switch embed := inode.Operations().(type) {
	case *dirInode:
		embed.mu.Lock()
		embed.n = fsNode.(fsnode.Parent)
		embed.mu.Unlock()
	case *regInode:
		embed.mu.Lock()
		embed.n = fsNode.(fsnode.Leaf)
		embed.mu.Unlock()
	default:
		log.Panicf("unexpected inodeEmbedder: %T", embed)
	}
}
