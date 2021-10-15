package fsnode

import (
	"bytes"
	"context"
	"os"

	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/base/ioctx/fsctx"
)

type funcLeaf struct {
	FileInfo
	open func(ctx context.Context) (fsctx.File, error)
}

// FuncLeaf constructs a Leaf from an open function.
// It's invoked every time; implementations should do their own caching if desired.
func FuncLeaf(info FileInfo, open func(ctx context.Context) (fsctx.File, error)) Leaf {
	return funcLeaf{info, open}
}
func (l funcLeaf) Open(ctx context.Context) (fsctx.File, error) { return l.open(ctx) }
func (l funcLeaf) FSNodeT()                                     {}

type (
	readerAtLeaf struct {
		FileInfo
		ioctx.ReaderAt
	}
	readerAtFile struct {
		readerAtLeaf
		off int64
	}
)

// ReaderAtLeaf constructs a Leaf whose file reads from r.
// Cacheability of both metadata and content is governed by info.
func ReaderAtLeaf(info FileInfo, r ioctx.ReaderAt) Leaf { return readerAtLeaf{info, r} }

func (r readerAtLeaf) Open(context.Context) (fsctx.File, error) { return &readerAtFile{r, 0}, nil }
func (l readerAtLeaf) FSNodeT()                                 {}

func (f *readerAtFile) Stat(context.Context) (os.FileInfo, error) {
	if f.ReaderAt == nil {
		return nil, os.ErrClosed
	}
	return f.FileInfo, nil
}
func (f *readerAtFile) Read(ctx context.Context, dst []byte) (int, error) {
	if f.ReaderAt == nil {
		return 0, os.ErrClosed
	}
	n, err := f.ReadAt(ctx, dst, f.off)
	f.off += int64(n)
	return n, err
}
func (f *readerAtFile) Close(context.Context) error {
	if f.ReaderAt == nil {
		return os.ErrClosed
	}
	f.ReaderAt = nil
	return nil
}

// ConstLeaf constructs a leaf with constant contents. Caller must not modify content after call.
// Uses content's size (ignoring existing info.Size).
func ConstLeaf(info FileInfo, content []byte) Leaf {
	info = info.WithSize(int64(len(content)))
	return ReaderAtLeaf(info, ioctx.FromStdReaderAt(bytes.NewReader(content)))
}

// TODO: From *os.File?
