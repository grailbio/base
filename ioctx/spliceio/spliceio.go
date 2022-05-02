package spliceio

import (
	"context"
	"os"

	"github.com/grailbio/base/ioctx/fsctx"
)

// ReaderAt reads data by giving the caller an OS file descriptor plus coordinates so the
// caller can directly splice (or just read) the file descriptor. Concurrent calls are allowed.
//
// It's possible for gotSize to be less than wantSize with nil error. This is different from
// io.ReaderAt. Callers should simply continue their read at off + gotSize.
type ReaderAt interface {
	// SpliceReadAt returns a file descriptor and coordinates, or error.
	//
	// Note that fdOff is totally unrelated to off; fdOff must only be used for operations on fd.
	// No guarantees are made about fd from different calls (no consistency, no uniqueness).
	// No guarantees are made about fdOff from different calls (no ordering, no uniqueness).
	SpliceReadAt(_ context.Context, wantSize int, off int64) (fd uintptr, gotSize int, fdOff int64, _ error)
}

// OSFile is a ReaderAt wrapping os.File. It's also a fsctx.File and a
// fsnodefuse.Writable.
type OSFile os.File

var (
	_ fsctx.File = (*OSFile)(nil)
	_ ReaderAt   = (*OSFile)(nil)
)

func (f *OSFile) SpliceReadAt(
	_ context.Context, wantSize int, off int64,
) (
	fd uintptr, gotSize int, fdOff int64, _ error,
) {
	// TODO: Validation? Probably don't need to check file size. Maybe wantSize, off >= 0?
	return (*os.File)(f).Fd(), wantSize, off, nil
}

func (f *OSFile) Stat(context.Context) (os.FileInfo, error)     { return (*os.File)(f).Stat() }
func (f *OSFile) Read(_ context.Context, b []byte) (int, error) { return (*os.File)(f).Read(b) }
func (f *OSFile) Close(context.Context) error                   { return (*os.File)(f).Close() }

func (f *OSFile) WriteAt(_ context.Context, b []byte, offset int64) (int, error) {
	return (*os.File)(f).WriteAt(b, offset)
}
func (f *OSFile) Truncate(_ context.Context, size int64) error { return (*os.File)(f).Truncate(size) }
func (f *OSFile) Flush(_ context.Context) error                { return nil }
func (f *OSFile) Fsync(_ context.Context) error                { return (*os.File)(f).Sync() }
