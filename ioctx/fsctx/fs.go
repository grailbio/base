// fsctx adds context.Context to io/fs APIs.
//
// TODO: Specify policy for future additions to this package. See ioctx.
package fsctx

import (
	"context"
	"os"
)

// FS is io/fs.FS with context added.
type FS interface {
	Open(_ context.Context, name string) (File, error)
}

// File is io/fs.File with context added.
type File interface {
	Stat(context.Context) (os.FileInfo, error)
	Read(context.Context, []byte) (int, error)
	Close(context.Context) error
}

// DirEntry is io/fs.DirEntry with context added.
type DirEntry interface {
	Name() string
	IsDir() bool
	Type() os.FileMode
	Info(context.Context) (os.FileInfo, error)
}

// ReadDirFile is io/fs.ReadDirFile with context added.
type ReadDirFile interface {
	File
	ReadDir(_ context.Context, n int) ([]DirEntry, error)
}
