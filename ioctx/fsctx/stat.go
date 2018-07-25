package fsctx

import (
	"context"
	"os"
)

// StatFS is io/fs.StatFS with context added.
type StatFS interface {
	FS
	Stat(_ context.Context, name string) (os.FileInfo, error)
}

// Stat is io/fs.Stat with context added.
func Stat(ctx context.Context, fsys FS, name string) (os.FileInfo, error) {
	if fsys, ok := fsys.(StatFS); ok {
		return fsys.Stat(ctx, name)
	}

	file, err := fsys.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close(ctx) }()
	return file.Stat(ctx)
}
