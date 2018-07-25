package s3file

import (
	"context"
	"os"
	"sync/atomic"
	"unsafe"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/grail/biofs/biofseventlog"
	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/base/ioctx/fsctx"
)

type (
	versionsLeaf struct {
		fsnode.FileInfo
		s3Query
		versionID string
	}
	versionsFile struct {
		versionsLeaf

		// readOffset is the cursor for Read().
		readOffset int64

		reader chunkReaderCache
	}
)

var (
	_ fsnode.Leaf    = versionsLeaf{}
	_ fsctx.File     = (*versionsFile)(nil)
	_ ioctx.ReaderAt = (*versionsFile)(nil)
)

func (n versionsLeaf) FSNodeT() {}

func (n versionsLeaf) OpenFile(ctx context.Context, flag int) (fsctx.File, error) {
	biofseventlog.UsedFeature("s3.versions.open")
	return &versionsFile{versionsLeaf: n}, nil
}

func (f *versionsFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return f.FileInfo, nil
}

func (f *versionsFile) Read(ctx context.Context, dst []byte) (int, error) {
	n, err := f.ReadAt(ctx, dst, f.readOffset)
	f.readOffset += int64(n)
	return n, err
}

func (f *versionsFile) ReadAt(ctx context.Context, dst []byte, offset int64) (int, error) {
	reader, cleanUp, err := f.reader.getOrCreate(ctx, func() (*chunkReaderAt, error) {
		clients, err := f.impl.clientsForAction(ctx, "GetObjectVersion", f.bucket, f.key)
		if err != nil {
			return nil, errors.E(err, "getting clients")
		}
		return &chunkReaderAt{
			name: f.path(), bucket: f.bucket, key: f.key, versionID: f.versionID,
			newRetryPolicy: func() retryPolicy {
				return newBackoffPolicy(append([]s3iface.S3API{}, clients...), file.Opts{})
			},
		}, nil
	})
	if err != nil {
		return 0, err
	}
	defer cleanUp()
	// TODO: Consider checking s3Info for ETag changes.
	n, _, err := reader.ReadAt(ctx, dst, offset)
	return n, err
}

func (f *versionsFile) Close(ctx context.Context) error {
	f.reader.close()
	return nil
}

type chunkReaderCache struct {
	// available is idle (for some goroutine to use). Goroutines set available = nil before
	// using it to "acquire" it, then return it after their operation (if available == nil then).
	// If the caller only uses one thread, we'll end up creating and reusing just one
	// *chunkReaderAt for all operations.
	available unsafe.Pointer // *chunkReaderAt
}

// get constructs a reader. cleanUp must be called iff error is nil.
func (c *chunkReaderCache) getOrCreate(
	ctx context.Context, create func() (*chunkReaderAt, error),
) (
	reader *chunkReaderAt, cleanUp func(), err error,
) {
	trySaveReader := func() {
		if atomic.CompareAndSwapPointer(&c.available, nil, unsafe.Pointer(reader)) {
			return
		}
		reader.Close()
	}

	reader = (*chunkReaderAt)(atomic.SwapPointer(&c.available, nil))
	if reader != nil {
		return reader, trySaveReader, nil
	}

	reader, err = create()
	if err != nil {
		if reader != nil {
			reader.Close()
		}
		return nil, nil, err
	}

	return reader, trySaveReader, nil
}

func (c *chunkReaderCache) close() {
	reader := (*chunkReaderAt)(atomic.SwapPointer(&c.available, nil))
	if reader != nil {
		reader.Close()
	}
}
