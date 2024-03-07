// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package gfilefs_test

import (
	"context"
	"flag"
	gofs "io/fs"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/fsnodefuse"
	"github.com/grailbio/base/file/gfilefs"
	"github.com/grailbio/base/file/s3file"
	"github.com/grailbio/testutil"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	file.RegisterImplementation("s3", func() file.Implementation {
		return s3file.NewImplementation(
			s3file.NewDefaultProvider(), s3file.Options{},
		)
	})
}

// s3RootFlag sets an S3 root directory to use for test files.  When set to a
// non-empty S3 path, e.g. "s3://some-bucket/some-writable/prefix", tests will
// run with a mount point with this root.  These tests will run in addition to
// the normal local root testing.
var s3RootFlag = flag.String(
	"s3-root",
	"",
	"optional S3 root directory to use for testing, in addition to the local root",
)

// TestCreateEmpty verifies that we can create an new empty file using various
// flag parameters when opening, e.g. O_TRUNC.
func TestCreateEmpty(t *testing.T) {
	flagElements := [][]int{
		{os.O_RDONLY, os.O_RDWR, os.O_WRONLY},
		{0, os.O_TRUNC},
		{0, os.O_EXCL},
	}
	// combos produces the flag parameters to test (less O_CREATE, which is
	// applied below).
	var combos func(elems [][]int) []int
	combos = func(elems [][]int) []int {
		if len(elems) == 1 {
			return elems[0]
		}
		var result []int
		for _, elem := range elems[0] {
			for _, flag := range combos(elems[1:]) {
				flag |= elem
				result = append(result, flag)
			}
		}
		return result
	}
	// name generates a nice name for a subtest for a given flag.
	name := func(flags int) string {
		var (
			parts  []string
			access string
		)
		switch {
		case flags&os.O_RDWR == os.O_RDWR:
			access = "RDWR"
		case flags&os.O_WRONLY == os.O_WRONLY:
			access = "WRONLY"
		default:
			access = "RDONLY"
		}
		parts = append(parts, access)
		if flags&os.O_TRUNC == os.O_TRUNC {
			parts = append(parts, "TRUNC")
		}
		if flags&os.O_EXCL == os.O_EXCL {
			parts = append(parts, "EXCL")
		}
		return strings.Join(parts, "_")
	}
	for _, flag := range combos(flagElements) {
		withTestMounts(t, func(m testMount) {
			t.Run(name(flag), func(t *testing.T) {
				path := filepath.Join(m.mountPoint, "test")
				flag |= os.O_CREATE
				f, err := os.OpenFile(path, flag, 0666)
				require.NoError(t, err, "creating file")
				require.NoError(t, f.Close(), "closing file")

				info, err := os.Stat(path)
				require.NoError(t, err, "stat of file")
				assert.Equal(t, int64(0), info.Size(), "file should have zero size")

				bs, err := ioutil.ReadFile(path)
				require.NoError(t, err, "reading file")
				assert.Empty(t, bs, "file should be empty")
			})
		})
	}
}

// TestCreate verifies that we can create a new file, write content to it, and
// read the same content back.
func TestCreate(t *testing.T) {
	withTestMounts(t, func(m testMount) {
		var (
			r        = rand.New(rand.NewSource(0))
			path     = filepath.Join(m.mountPoint, "test")
			rootPath = file.Join(m.root, "test")
		)
		assertRoundTrip(t, path, rootPath, r, 10*(1<<20))
		assertRoundTrip(t, path, rootPath, r, 10*(1<<16))
	})
}

// TestOverwrite verifies that we can overwrite the same file repeatedly, and
// that the updated content is correct.
func TestOverwrite(t *testing.T) {
	withTestMounts(t, func(m testMount) {
		const NumOverwrites = 20
		var (
			r        = rand.New(rand.NewSource(0))
			path     = filepath.Join(m.mountPoint, "test")
			rootPath = file.Join(m.root, "test")
		)
		for i := 0; i < NumOverwrites+1; i++ {
			// Each iteration uses a random size between 5 and 10 MiB.
			n := 5 + r.Intn(10)
			n *= 1 << 20
			assertRoundTrip(t, path, rootPath, r, n)
		}
	})
}

// TestTruncFlag verifies that opening with O_TRUNC truncates the file.
func TestTruncFlag(t *testing.T) {
	t.Run("WRONLY", func(t *testing.T) {
		testTruncFlag(t, os.O_WRONLY)
	})
	t.Run("RDWR", func(t *testing.T) {
		testTruncFlag(t, os.O_RDWR)
	})
}

func testTruncFlag(t *testing.T, flag int) {
	withTestMounts(t, func(m testMount) {
		path := filepath.Join(m.mountPoint, "test")
		// Write the file we will truncate to test.
		err := ioutil.WriteFile(path, []byte{0, 1, 2}, 0644)
		require.NoError(t, err, "writing file")

		f, err := os.OpenFile(path, flag|os.O_TRUNC, 0666)
		require.NoError(t, err, "opening for truncation")
		func() {
			defer func() {
				require.NoError(t, f.Close())
			}()
			var info gofs.FileInfo
			info, err = f.Stat()
			require.NoError(t, err, "getting file stats")
			assert.Equal(t, int64(0), info.Size(), "truncated file should be zero bytes")
		}()

		// Verify that reading the truncated file yields zero bytes.
		bsRead, err := ioutil.ReadFile(path)
		require.NoError(t, err, "reading truncated file")
		assert.Empty(t, bsRead, "reading truncated file should yield no data")
	})
}

// TestTruncateZero verifies that truncation to zero works.
func TestTruncateZero(t *testing.T) {
	t.Run("WRONLY", func(t *testing.T) {
		testTruncateZero(t, os.O_WRONLY)
	})
	t.Run("RDWR", func(t *testing.T) {
		testTruncateZero(t, os.O_RDWR)
	})
}

func testTruncateZero(t *testing.T, flag int) {
	withTestMounts(t, func(m testMount) {
		path := filepath.Join(m.mountPoint, "test")
		// Write the file we will truncate to test.
		err := ioutil.WriteFile(path, []byte{0, 1, 2}, 0644)
		require.NoError(t, err, "writing file")

		f, err := os.OpenFile(path, os.O_WRONLY, 0666)
		require.NoError(t, err, "opening for truncation")

		func() {
			defer func() {
				require.NoError(t, f.Close(), "closing")
			}()
			// Sanity check that the initial file handle is the correct size.
			var info gofs.FileInfo
			info, err = f.Stat()
			require.NoError(t, err, "getting file stats")
			assert.Equal(t, int64(3), info.Size(), "file to truncate should be three bytes")

			require.NoError(t, f.Truncate(0), "truncating")

			// Verify that the file handle is actually truncated.
			info, err = f.Stat()
			require.NoError(t, err, "getting file stats")
			assert.Equal(t, int64(0), info.Size(), "truncated file should be zero bytes")
		}()

		// Verify that an independent stat shows zero size.
		info, err := os.Stat(path)
		require.NoError(t, err, "getting file stats")
		assert.Equal(t, int64(0), info.Size(), "truncated file should be zero bytes")

		// Verify that reading the truncated file yields zero bytes.
		bsRead, err := ioutil.ReadFile(path)
		require.NoError(t, err, "reading truncated file")
		assert.Empty(t, bsRead, "reading truncated file should yield no data")
	})
}

// TestRemove verifies that we can remove a file.
func TestRemove(t *testing.T) {
	withTestMounts(t, func(m testMount) {
		var (
			r        = rand.New(rand.NewSource(0))
			path     = filepath.Join(m.mountPoint, "test")
			rootPath = file.Join(m.root, "test")
		)
		bs := make([]byte, 1*(1<<20))
		_, err := r.Read(bs)
		require.NoError(t, err, "making random data")
		err = ioutil.WriteFile(path, bs, 0644)
		require.NoError(t, err, "writing file")
		err = os.Remove(path)
		require.NoError(t, err, "removing file")
		_, err = os.Stat(path)
		require.True(t, os.IsNotExist(err), "file was not removed")
		_, err = os.Stat(rootPath)
		require.True(t, os.IsNotExist(err), "file was not removed in root")
	})
}

// TestDirListing verifies that the directory listing of a file is updated when
// the file is modified.
func TestDirListing(t *testing.T) {
	withTestMounts(t, func(m testMount) {
		path := file.Join(m.mountPoint, "test")
		// assertSize asserts that the listed FileInfo of the file at path reports
		// the given size.
		assertSize := func(size int64) {
			infos, err := ioutil.ReadDir(m.mountPoint)
			require.NoError(t, err, "listing directory")
			require.Equal(t, 1, len(infos), "should only be one file in directory")
			assert.Equal(t, size, infos[0].Size(), "file should be 3 bytes")
		}

		// Write a 3-byte file, and verify that its listing has the correct size.
		require.NoError(t, ioutil.WriteFile(path, make([]byte, 3), 0644), "writing file")
		assertSize(3)

		// Overwrite it to be 1 byte, and verify that the listing is updated.
		require.NoError(t, ioutil.WriteFile(path, make([]byte, 1), 0644), "overwriting file")
		assertSize(1)

		// Append 3 bytes, and verify that the listing is updated.
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
		require.NoError(t, err, "opening file for append")
		_, err = f.Write(make([]byte, 3))
		require.NoError(t, err, "appending to file")
		require.NoError(t, f.Close(), "closing file")
		assertSize(4)
	})
}

// TestMkdir verifies that we can make a directory.
func TestMkdir(t *testing.T) {
	withTestMounts(t, func(m testMount) {
		var (
			r    = rand.New(rand.NewSource(0))
			path = filepath.Join(m.mountPoint, "test-dir")
		)
		err := os.Mkdir(path, 0775)
		require.NoError(t, err, "making directory")

		filePath := filepath.Join(path, "test")
		rootFilePath := file.Join(m.root, "test-dir", "test")
		assertRoundTrip(t, filePath, rootFilePath, r, 0)

		info, err := os.Stat(path)
		require.NoError(t, err, "getting file info of created directory")
		require.True(t, info.IsDir(), "created directory is not a directory")
	})
}

func withTestMounts(t *testing.T, f func(m testMount)) {
	type makeRootFunc func(*testing.T) (string, func())
	makeRoots := map[string]makeRootFunc{
		"local": func(t *testing.T) (string, func()) {
			return testutil.TempDir(t, "", "gfilefs-mnt")
		},
	}
	if *s3RootFlag != "" {
		makeRoots["s3"] = func(t *testing.T) (string, func()) {
			ctx := context.Background()
			lister := file.List(ctx, *s3RootFlag, true)
			exists := lister.Scan()
			if exists {
				t.Logf("path exists: %s", lister.Path())
			}
			require.NoErrorf(t, lister.Err(), "listing %s", *s3RootFlag)
			require.False(t, exists)
			return *s3RootFlag, func() {
				err := forEachFile(ctx, *s3RootFlag, func(path string) error {
					return file.Remove(ctx, path)
				})
				require.NoError(t, err, "cleaning up test root")
			}
		}
	}
	for name, makeRoot := range makeRoots {
		t.Run(name, func(t *testing.T) {
			root, rootCleanUp := makeRoot(t)
			defer rootCleanUp()
			mountPoint, mountPointCleanUp := testutil.TempDir(t, "", "gfilefs-mnt")
			defer mountPointCleanUp()
			server, err := fs.Mount(
				mountPoint,
				fsnodefuse.NewRoot(gfilefs.New(root, "root")),
				// TODO: Set fsnodefuse.ConfigureRequiredMountOptions.
				&fs.Options{
					MountOptions: fuse.MountOptions{
						FsName:        "test",
						DisableXAttrs: true,
						Debug:         true,
						MaxBackground: 1024,
					},
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
			f(testMount{root: root, mountPoint: mountPoint})
		})
	}
}

type testMount struct {
	// root is the root path that is mounted at dir.
	root string
	// mountPoint is the FUSE mount point.
	mountPoint string
}

// forEachFile runs the callback for every file under the directory in
// parallel.  It returns any of the errors returned by the callback.  It is
// cribbed from github.com/grailbio/base/cmd/grail-file/cmd.
func forEachFile(ctx context.Context, dir string, callback func(path string) error) error {
	const parallelism = 32
	err := errors.Once{}
	wg := sync.WaitGroup{}
	ch := make(chan string, parallelism*100)
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			for path := range ch {
				err.Set(callback(path))
			}
			wg.Done()
		}()
	}

	lister := file.List(ctx, dir, true /*recursive*/)
	for lister.Scan() {
		if !lister.IsDir() {
			ch <- lister.Path()
		}
	}
	close(ch)
	err.Set(lister.Err())
	wg.Wait()
	return err.Err()
}

func assertRoundTrip(t *testing.T, path, rootPath string, r *rand.Rand, size int) {
	bs := make([]byte, size)
	_, err := r.Read(bs)
	require.NoError(t, err, "making random data")
	err = ioutil.WriteFile(path, bs, 0644)
	require.NoError(t, err, "writing file")

	got, err := ioutil.ReadFile(path)
	require.NoError(t, err, "reading file back")
	assert.Equal(t, bs, got, "data read != data written")

	info, err := os.Stat(path)
	require.NoError(t, err, "stat of file")
	assert.Equal(t, int64(len(bs)), info.Size(), "len(data read) != len(data written)")

	// Verify that the file is written correctly to mounted root.
	got, err = file.ReadFile(context.Background(), rootPath)
	require.NoErrorf(t, err, "reading file in root %s back", rootPath)
	assert.Equal(t, bs, got, "data read != data written")
}
