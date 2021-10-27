package fsnodefuse

import (
	"github.com/grailbio/base/file/fsnode"
	"github.com/hanwen/go-fuse/v2/fs"
)

// inodeEmbedder embeds fs.InodeEmbedder and adds a method to return the
// wrapped fsnode.T the embedder represents.
type inodeEmbedder interface {
	fs.InodeEmbedder
	// fsNode returns the fsnode.T this embedder represents.
	fsNode() fsnode.T
}
