package readmatcher_test

import (
	"bytes"
	"flag"
	"io"
	"math/rand"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/file/fsnodefuse"
	"github.com/grailbio/base/file/internal/readmatcher"
	"github.com/grailbio/base/file/internal/readmatcher/readmatchertest"
	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/assert"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

var (
	dataBytes         = flag.Int("data-bytes", 1<<27, "read corpus size")
	stressParallelism = flag.Int("stress-parallelism", runtime.NumCPU(),
		"number of parallel readers during stress test")
	fuseFlag = flag.Bool("fuse", false, "create a temporary FUSE mount and test through that")
)

func TestStress(t *testing.T) {
	var data = make([]byte, *dataBytes)
	_, _ = rand.New(rand.NewSource(1)).Read(data)
	offsetReader := func(start int64) ioctx.ReadCloser {
		return ioctx.FromStdReadCloser(io.NopCloser(bytes.NewReader(data[start:])))
	}
	type fuseCase struct {
		name string
		test func(*testing.T, ioctx.ReaderAt)
	}
	fuseCases := []fuseCase{
		{
			"nofuse",
			func(t *testing.T, r ioctx.ReaderAt) {
				readmatchertest.Stress(data, r, *stressParallelism)
			},
		},
	}
	if *fuseFlag {
		fuseCases = append(fuseCases, fuseCase{
			"fuse",
			func(t *testing.T, rAt ioctx.ReaderAt) {
				mountPoint, cleanUpMountPoint := testutil.TempDir(t, "", "readmatcher_test")
				defer cleanUpMountPoint()
				const filename = "data"
				server, err := fs.Mount(
					mountPoint,
					fsnodefuse.NewRoot(fsnode.NewParent(
						fsnode.NewDirInfo("root"),
						fsnode.ConstChildren(
							fsnode.ReaderAtLeaf(
								fsnode.NewRegInfo(filename).WithSize(int64(len(data))),
								rAt,
							),
						),
					)),
					&fs.Options{
						MountOptions: func() fuse.MountOptions {
							opts := fuse.MountOptions{FsName: "test", Debug: log.At(log.Debug)}
							fsnodefuse.ConfigureRequiredMountOptions(&opts)
							fsnodefuse.ConfigureDefaultMountOptions(&opts)
							return opts
						}(),
					},
				)
				require.NoError(t, err, "mounting %q", mountPoint)
				defer func() {
					log.Printf("unmounting %q", mountPoint)
					assert.NoError(t, server.Unmount(),
						"unmount of FUSE mounted at %q failed; may need manual cleanup",
						mountPoint,
					)
					log.Printf("unmounted %q", mountPoint)
				}()
				f, err := os.Open(path.Join(mountPoint, filename))
				require.NoError(t, err)
				defer func() { require.NoError(t, f.Close()) }()
				readmatchertest.Stress(data, ioctx.FromStdReaderAt(f), *stressParallelism)
			},
		})
	}
	for _, c := range fuseCases {
		t.Run(c.name, func(t *testing.T) {
			t.Run("less parallelism", func(t *testing.T) {
				readerParallelism := *stressParallelism / 2
				must.True(readerParallelism > 0)
				m := readmatcher.New(offsetReader, readmatcher.SoftMaxReaders(readerParallelism))
				c.test(t, m)
			})
			t.Run("more parallelism", func(t *testing.T) {
				readerParallelism := 2 * *stressParallelism
				m := readmatcher.New(offsetReader, readmatcher.SoftMaxReaders(readerParallelism))
				c.test(t, m)
			})
		})
	}
}
