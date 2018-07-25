package unzipfs

import (
	"archive/zip"
	"compress/flate"
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"runtime"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file/addfs"
	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/grail/biofs/biofseventlog"
	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/base/ioctx/fsctx"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/morebufio"
	"github.com/grailbio/base/sync/loadingcache"
)

type unzipFunc struct{}

// Func is an addfs.PerNodeFunc that presents zip file contents as a subdirectory tree.
// Users can access contents in .../myfile.zip/unzip/, for example.
//
// The file need not have extension .zip. Func.Apply reads the file header and if it's not
// a supported zip file the unzip/ directory is omitted.
var Func unzipFunc

var _ addfs.PerNodeFunc = Func

func (unzipFunc) Apply(ctx context.Context, node fsnode.T) ([]fsnode.T, error) {
	zipLeaf, ok := node.(fsnode.Leaf)
	if !ok {
		return nil, nil
	}
	info := fsnode.NewDirInfo("unzip").WithCacheableFor(fsnode.CacheableFor(zipLeaf))
	parent, err := parentFromLeaf(ctx, info, zipLeaf)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		return nil, nil
	}
	return []fsnode.T{parent}, nil
}

type readerHandle struct {
	*zip.Reader
	ioctx.Closer
	leaf fsnode.Leaf
}

func finalizeHandle(h *readerHandle) {
	if err := h.Close(context.Background()); err != nil {
		log.Error.Printf("unzipfs: error closing handle: %v", err)
	}
}

// parentFromLeaf opens zipLeaf to determine if it's a zip file and returns a Parent if so.
// Returns nil, nil in cases where the file is not supported (like, not a zip file).
// TODO: Consider exposing more public APIs like this fsnode.Leaf -> fsnode.Parent and/or
// *zip.Reader -> fsnode.Parent.
func parentFromLeaf(ctx context.Context, parentInfo fsnode.FileInfo, zipLeaf fsnode.Leaf) (fsnode.Parent, error) {
	zipFile, err := fsnode.Open(ctx, zipLeaf)
	if err != nil {
		return nil, errors.E(err, "opening for unzip")
	}
	handle := readerHandle{Closer: zipFile, leaf: zipLeaf}
	// TODO: More reliable/explicit cleanup. Refcount?
	runtime.SetFinalizer(&handle, finalizeHandle)
	info, err := zipFile.Stat(ctx)
	if err != nil {
		return nil, errors.E(err, "stat-ing for unzip")
	}
	rAt, ok := zipFile.(ioctx.ReaderAt)
	if !ok {
		log.Info.Printf("zipfs: random access not supported: %s, returning empty dir", zipLeaf.Info().Name())
		// TODO: Some less efficient fallback path? Try seeking?
		return nil, nil
	}
	// Buffer makes header read much faster when underlying file is high latency, like S3.
	// Of course there's a tradeoff where very small zip files (with much smaller headers) will
	// not be read as lazily, but the speedup is significant for S3.
	rAt = morebufio.NewReaderAtSize(rAt, 1024*1024)
	handle.Reader, err = zip.NewReader(ioctx.ToStdReaderAt(ctx, rAt), info.Size())
	if err != nil {
		if stderrors.Is(err, zip.ErrFormat) ||
			stderrors.Is(err, zip.ErrAlgorithm) ||
			stderrors.Is(err, zip.ErrChecksum) {
			log.Info.Printf("zipfs: not a valid zip file: %s, returning empty dir", zipLeaf.Info().Name())
			return nil, nil
		}
		return nil, errors.E(err, "initializing zip reader")
	}
	return fsnode.NewParent(parentInfo, &handleChildGen{r: &handle, pathPrefix: "."}), nil
}

type handleChildGen struct {
	r          *readerHandle
	pathPrefix string
	children   loadingcache.Value
}

func (g *handleChildGen) GenerateChildren(ctx context.Context) ([]fsnode.T, error) {
	biofseventlog.UsedFeature("unzipfs.children")
	var children []fsnode.T
	err := g.children.GetOrLoad(ctx, &children, func(ctx context.Context, opts *loadingcache.LoadOpts) error {
		entries, err := fs.ReadDir(g.r, g.pathPrefix)
		if err != nil {
			return err
		}
		children = make([]fsnode.T, len(entries))
		cacheFor := fsnode.CacheableFor(g.r.leaf)
		for i, entry := range entries {
			stat, err := entry.Info() // Immediate (no additional file read) as of go1.17.
			if err != nil {
				return errors.E(err, fmt.Sprintf("stat: %s", entry.Name()))
			}
			childInfo := fsnode.CopyFileInfo(stat).WithCacheableFor(cacheFor)
			fullName := path.Join(g.pathPrefix, entry.Name())
			if entry.IsDir() {
				children[i] = fsnode.NewParent(childInfo, &handleChildGen{r: g.r, pathPrefix: fullName})
			} else {
				children[i] = zipFileLeaf{g.r, childInfo, fullName}
			}
		}
		opts.CacheFor(cacheFor)
		return nil
	})
	if err != nil {
		return nil, errors.E(err, fmt.Sprintf("listing path: %s", g.pathPrefix))
	}
	return children, nil
}

type zipFileLeaf struct {
	r *readerHandle
	fsnode.FileInfo
	zipName string
}

var _ fsnode.Leaf = (*zipFileLeaf)(nil)

func (z zipFileLeaf) FSNodeT() {}

type zipFileLeafFile struct {
	info fsnode.FileInfo

	// semaphore guards all subsequent fields. It's used to serialize operations.
	semaphore chan struct{}
	// stdRAt translates context-less ReadAt requests into context-ful ones. We serialize Read
	// requests (with semaphore) and then set stdRAt.Ctx temporarily to allow cancellation.
	stdRAt ioctx.StdReaderAt
	// stdRC wraps stdRAt. Its operations don't directly accept a context but are subject to
	// cancellation indirectly via the inner stdRAt.
	stdRC io.ReadCloser
	// fileCloser cleans up.
	fileCloser ioctx.Closer
}

func (z zipFileLeaf) OpenFile(ctx context.Context, flag int) (fsctx.File, error) {
	biofseventlog.UsedFeature("unzipfs.open")
	var fileEntry *zip.File
	for _, f := range z.r.File {
		if f.Name == z.zipName {
			fileEntry = f
			break
		}
	}
	if fileEntry == nil {
		return nil, errors.E(errors.NotExist,
			fmt.Sprintf("internal inconsistency: entry %q not found in zip metadata", z.zipName))
	}
	dataOffset, err := fileEntry.DataOffset()
	if err != nil {
		return nil, errors.E(err, fmt.Sprintf("could not get data offset for %s", fileEntry.Name))
	}
	var makeDecompressor func(r io.Reader) io.ReadCloser
	switch fileEntry.Method {
	case zip.Store:
		// TODO: Consider returning a ReaderAt in this case for user convenience.
		makeDecompressor = io.NopCloser
	case zip.Deflate:
		makeDecompressor = flate.NewReader
	default:
		return nil, errors.E(errors.NotSupported,
			fmt.Sprintf("unsupported method: %d for: %s", fileEntry.Method, fileEntry.Name))
	}
	zipFile, err := fsnode.Open(ctx, z.r.leaf)
	if err != nil {
		return nil, err
	}
	rAt, ok := zipFile.(ioctx.ReaderAt)
	if !ok {
		err := errors.E(errors.NotSupported, fmt.Sprintf("not ReaderAt: %v", zipFile))
		errors.CleanUpCtx(ctx, zipFile.Close, &err)
		return nil, err
	}
	f := zipFileLeafFile{
		info:       z.FileInfo,
		semaphore:  make(chan struct{}, 1),
		stdRAt:     ioctx.StdReaderAt{Ctx: ctx, ReaderAt: rAt},
		fileCloser: zipFile,
	}
	defer func() { f.stdRAt.Ctx = nil }()
	f.stdRC = makeDecompressor(
		io.NewSectionReader(&f.stdRAt, dataOffset, int64(fileEntry.CompressedSize64)))
	return &f, nil
}

func (f *zipFileLeafFile) Stat(context.Context) (fs.FileInfo, error) { return f.info, nil }

func (f *zipFileLeafFile) Read(ctx context.Context, dst []byte) (int, error) {
	select {
	case f.semaphore <- struct{}{}:
		defer func() { _ = <-f.semaphore }()
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	f.stdRAt.Ctx = ctx
	defer func() { f.stdRAt.Ctx = nil }()
	return f.stdRC.Read(dst)
}

func (f *zipFileLeafFile) Close(ctx context.Context) error {
	select {
	case f.semaphore <- struct{}{}:
		defer func() { _ = <-f.semaphore }()
	case <-ctx.Done():
		return ctx.Err()
	}

	f.stdRAt.Ctx = ctx
	defer func() { f.stdRAt = ioctx.StdReaderAt{} }()
	var err error
	if f.stdRC != nil {
		errors.CleanUp(f.stdRC.Close, &err)
		f.stdRC = nil
	}
	if f.fileCloser != nil {
		errors.CleanUpCtx(ctx, f.fileCloser.Close, &err)
		f.fileCloser = nil
	}
	return err
}
