// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package testutil

import (
	"context"
	"io"
	"io/ioutil"
	"sort"
	"testing"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/testutil/assert"
)

func doRead(t *testing.T, r io.Reader, len int) string {
	data := make([]byte, len)
	n, err := io.ReadFull(r, data)
	assert.EQ(t, len, n)
	if err == io.EOF {
		assert.EQ(t, 0, n)
	} else {
		assert.NoError(t, err)
	}
	return string(data)
}

func doReadAll(t *testing.T, r io.Reader) string {
	data, err := ioutil.ReadAll(r)
	assert.NoError(t, err)
	return string(data)
}

func doSeek(t *testing.T, r io.Seeker, off int64, whence int) {
	n, err := r.Seek(off, whence)
	assert.NoError(t, err)
	if whence == io.SeekStart {
		assert.EQ(t, n, off)
	}
}

func doReadFile(ctx context.Context, t *testing.T, impl file.Implementation, path string) string {
	f, err := impl.Open(ctx, path)
	assert.NoError(t, err, "open: %v", path)
	data := doReadAll(t, f.Reader(ctx))
	assert.NoError(t, f.Close(ctx))
	return data
}

func doWriteFile(ctx context.Context, t *testing.T, impl file.Implementation, path string, data string) {
	f, err := impl.Create(ctx, path)
	assert.NoError(t, err, "create: %v", path)
	w := f.Writer(ctx)
	_, err = w.Write([]byte(data))
	assert.NoError(t, err)
	assert.NoError(t, f.Close(ctx))
}

func fileExists(ctx context.Context, impl file.Implementation, path string) bool {
	_, err := impl.Stat(ctx, path)
	if err != nil && !errors.Is(errors.NotExist, err) {
		panic(err)
	}
	return err == nil
}

// TestEmpty creates an empty file and tests its operations.
func TestEmpty(
	ctx context.Context,
	t *testing.T,
	impl file.Implementation,
	path string) {
	f, err := impl.Create(ctx, path)
	assert.NoError(t, err)
	assert.NoError(t, err)
	assert.NoError(t, f.Close(ctx))

	f, err = impl.Open(ctx, path)
	assert.NoError(t, err)
	assert.EQ(t, "", doReadAll(t, f.Reader(ctx)))
	assert.NoError(t, f.Close(ctx))

	// Seek past the end of the file.
	f, err = impl.Open(ctx, path)
	assert.NoError(t, err)
	r := f.Reader(ctx)
	off, err := r.Seek(10, io.SeekStart)
	assert.NoError(t, err)
	assert.EQ(t, int64(10), off)
	assert.EQ(t, "", doReadAll(t, f.Reader(ctx)))
	assert.NoError(t, f.Close(ctx))
}

// TestNotExist tests that the implementation behaves correctly
// for paths that do not exist.
func TestNotExist(
	ctx context.Context,
	t *testing.T,
	impl file.Implementation,
	path string) {
	_, err := impl.Open(ctx, path)
	assert.True(t, errors.Is(errors.NotExist, err))
	_, err = impl.Stat(ctx, path)
	assert.True(t, errors.Is(errors.NotExist, err))
}

// TestErrors tests handling of errors. "path" shouldn't exist.
func TestErrors(
	ctx context.Context,
	t *testing.T,
	impl file.Implementation,
	path string) {
	_, err := impl.Stat(ctx, path)
	assert.NotNil(t, err)
	f, err := impl.Open(ctx, path)
	if err == nil {
		// S3 allows opening an non-existent file. But Stat or any other operation
		// for such a file fails.
		_, err := f.Stat(ctx)
		t.Logf("errortest %s: stat error %v", path, err)
		assert.NotNil(t, err)
		assert.NoError(t, f.Close(ctx))
	}
}

// TestReads tests various combination of reads and seeks.
func TestReads(
	ctx context.Context,
	t *testing.T,
	impl file.Implementation,
	path string) {
	expected := "A purple fox jumped over a blue cat"
	doWriteFile(ctx, t, impl, path, expected)

	// Read everything.
	f, err := impl.Open(ctx, path)
	assert.NoError(t, err)
	assert.EQ(t, expected, doReadAll(t, f.Reader(ctx)))

	// Read in two chunks.
	r := f.Reader(ctx)
	doSeek(t, r, 0, io.SeekStart)
	assert.EQ(t, expected[:3], doRead(t, r, 3))
	assert.EQ(t, expected[3:], doReadAll(t, r))

	// Stat
	stat, err := f.Stat(ctx)
	assert.NoError(t, err)
	assert.EQ(t, int64(len(expected)), stat.Size())

	// Reading again should provide no data, since the seek pointer is at the end.
	r = f.Reader(ctx)
	assert.EQ(t, "", doReadAll(t, r))
	doSeek(t, r, 3, io.SeekStart)
	assert.EQ(t, expected[3:], doReadAll(t, r))

	// Read bytes 4-7.
	doSeek(t, r, 4, io.SeekStart)
	assert.EQ(t, expected[4:7], doRead(t, r, 3))

	// Seek beyond the end of the file.
	doSeek(t, r, int64(len(expected)+1), io.SeekStart)
	assert.EQ(t, "", doReadAll(t, r))

	// Seek to the beginning.
	doSeek(t, r, 0, io.SeekStart)
	assert.EQ(t, expected, doReadAll(t, r))

	// Seek twice to the same offset
	doSeek(t, r, 1, io.SeekStart)
	doSeek(t, r, 1, io.SeekStart)
	assert.EQ(t, expected[1:], doReadAll(t, r))

	doSeek(t, r, 8, io.SeekStart)
	doSeek(t, r, -6, io.SeekCurrent)
	assert.EQ(t, "purple", doRead(t, r, 6))

	doSeek(t, r, -3, io.SeekEnd)
	assert.EQ(t, "cat", doReadAll(t, r))
}

// TestWrites tests file Write functions.
func TestWrites(ctx context.Context, t *testing.T, impl file.Implementation, dir string) {
	path := dir + "/tmp.txt"
	_ = impl.Remove(ctx, path)

	f, err := impl.Create(ctx, path)
	assert.NoError(t, err)
	assert.EQ(t, f.Name(), path)
	w := f.Writer(ctx)
	n, err := w.Write([]byte("writetest"))
	assert.NoError(t, err)
	assert.EQ(t, n, 9)

	// The file shouldn't exist before we call Close.
	assert.False(t, fileExists(ctx, impl, path), "write %v", path)
	// After close, the file becomes visible.
	assert.NoError(t, f.Close(ctx))
	assert.True(t, fileExists(ctx, impl, path), "write %v", path)

	// Read the file back.
	assert.EQ(t, doReadFile(ctx, t, impl, path), "writetest")

	// Overwrite the file
	f, err = impl.Create(ctx, path)
	assert.NoError(t, err)
	w = f.Writer(ctx)
	n, err = w.Write([]byte("anotherwrite"))
	assert.NoError(t, err)
	assert.EQ(t, n, 12)

	// Before closing, the file should store old contents
	assert.EQ(t, doReadFile(ctx, t, impl, path), "writetest")

	// On close, the file is updated to the new contents.
	assert.NoError(t, f.Close(ctx))
	assert.EQ(t, doReadFile(ctx, t, impl, path), "anotherwrite")
}

func TestDiscard(ctx context.Context, t *testing.T, impl file.Implementation, dir string) {
	path := dir + "/tmp.txt"
	_ = impl.Remove(ctx, path)

	f, err := impl.Create(ctx, path)
	assert.NoError(t, err)
	w := f.Writer(ctx)
	_, err = w.Write([]byte("writetest"))
	assert.NoError(t, err)

	// Discard, and then make sure it doesn't exist.
	f.Discard(ctx)
	if fileExists(ctx, impl, path) {
		t.Errorf("path %s exists after call to discard", path)
	}
}

// TestRemove tests file Remove() function.
func TestRemove(ctx context.Context, t *testing.T, impl file.Implementation, path string) {
	doWriteFile(ctx, t, impl, path, "removetest")
	assert.True(t, fileExists(ctx, impl, path))
	assert.NoError(t, impl.Remove(ctx, path))
	assert.False(t, fileExists(ctx, impl, path))
}

// TestStat tests Stat method implementations.
func TestStat(ctx context.Context, t *testing.T, impl file.Implementation, path string) {
	// {min,max}ModTime define the range of reasonable modtime for the test file.
	// We allow for 1 minute slack to account for clock skew on the file server.
	minModTime := time.Now().Add(-60 * time.Second)
	doWriteFile(ctx, t, impl, path, "stattest0")

	dir := path + "dir"
	doWriteFile(ctx, t, impl, dir+"/file", "stattest1")
	maxModTime := time.Now().Add(60 * time.Second)

	f, err := impl.Open(ctx, path)
	assert.NoError(t, err)
	info, err := f.Stat(ctx)
	assert.NoError(t, f.Close(ctx))

	assert.NoError(t, err)
	assert.EQ(t, int64(9), info.Size())
	assert.True(t, info.ModTime().After(minModTime) && info.ModTime().Before(maxModTime),
		"Info: %+v, min %+v, max %+v", info.ModTime(), minModTime, maxModTime)

	info2, err := impl.Stat(ctx, path)
	assert.NoError(t, err)
	assert.EQ(t, info, info2)

	// Stat on directory is not supported.
	_, err = impl.Stat(ctx, dir)
	assert.NotNil(t, err)
}

type dirEntry struct {
	path string
	size int64
}

// TestList tests List implementations.
func TestList(ctx context.Context, t *testing.T, impl file.Implementation, dir string) {
	doList := func(prefix string) (ents []dirEntry) {
		lister := impl.List(ctx, prefix, true)
		for lister.Scan() {
			ents = append(ents, dirEntry{lister.Path(), lister.Info().Size()})
		}
		sort.Slice(ents, func(i, j int) bool { return ents[i].path < ents[j].path })
		return
	}
	doWriteFile(ctx, t, impl, dir+"/f0.txt", "f0")
	doWriteFile(ctx, t, impl, dir+"/g0.txt", "g12")
	doWriteFile(ctx, t, impl, dir+"/d0.txt", "d0e1")
	doWriteFile(ctx, t, impl, dir+"/d0/f2.txt", "d0/f23")
	doWriteFile(ctx, t, impl, dir+"/d0/d1/f3.txt", "d0/f345")

	assert.EQ(t, []dirEntry{
		dirEntry{dir + "/f0.txt", 2},
	}, doList(dir+"/f0.txt"))

	assert.EQ(t, []dirEntry{
		dirEntry{dir + "/d0.txt", 4},
		dirEntry{dir + "/d0/d1/f3.txt", 7},
		dirEntry{dir + "/d0/f2.txt", 6},
		dirEntry{dir + "/f0.txt", 2},
		dirEntry{dir + "/g0.txt", 3},
	}, doList(dir))

	// List only lists files under the given directory.
	// So listing "d0" should exclude d0.txt.
	assert.EQ(t, []dirEntry{
		dirEntry{dir + "/d0/d1/f3.txt", 7},
		dirEntry{dir + "/d0/f2.txt", 6},
	}, doList(dir+"/d0"))
	assert.EQ(t, []dirEntry{
		dirEntry{dir + "/d0/d1/f3.txt", 7},
		dirEntry{dir + "/d0/f2.txt", 6},
	}, doList(dir+"/d0/"))
}

// TestListDir tests ListDir implementations.
func TestListDir(ctx context.Context, t *testing.T, impl file.Implementation, dir string) {
	doList := func(prefix string) (ents []dirEntry) {
		lister := impl.List(ctx, prefix, false)
		for lister.Scan() {
			de := dirEntry{lister.Path(), 0}
			if !lister.IsDir() {
				de.size = lister.Info().Size()
			}
			ents = append(ents, de)
		}
		sort.Slice(ents, func(i, j int) bool { return ents[i].path < ents[j].path })
		return
	}
	doWriteFile(ctx, t, impl, dir+"/f0.txt", "f0")
	doWriteFile(ctx, t, impl, dir+"/g0.txt", "g12")
	doWriteFile(ctx, t, impl, dir+"/d0.txt", "d0e1")
	doWriteFile(ctx, t, impl, dir+"/d0/f2.txt", "d0/f23")
	doWriteFile(ctx, t, impl, dir+"/d0/d1/f3.txt", "d0/f345")

	assert.EQ(t, []dirEntry{
		dirEntry{dir + "/d0", 0},
		dirEntry{dir + "/d0.txt", 4},
		dirEntry{dir + "/f0.txt", 2},
		dirEntry{dir + "/g0.txt", 3},
	}, doList(dir))

	// List only lists files under the given directory.
	// So listing "d0" should exclude d0.txt.
	assert.EQ(t, []dirEntry{
		dirEntry{dir + "/d0/d1", 0},
		dirEntry{dir + "/d0/f2.txt", 6},
	}, doList(dir+"/d0"))
	assert.EQ(t, []dirEntry{
		dirEntry{dir + "/d0/d1", 0},
		dirEntry{dir + "/d0/f2.txt", 6},
	}, doList(dir+"/d0/"))
}

// TestAll runs all the tests in this package.
func TestAll(ctx context.Context, t *testing.T, impl file.Implementation, dir string) {
	iName := impl.String()

	t.Run(iName+"_Empty", func(t *testing.T) { TestEmpty(ctx, t, impl, dir+"/empty.txt") })
	t.Run(iName+"_NotExist", func(t *testing.T) { TestNotExist(ctx, t, impl, dir+"/notexist.txt") })
	t.Run(iName+"_Errors", func(t *testing.T) { TestErrors(ctx, t, impl, dir+"/errors.txt") })
	t.Run(iName+"_Reads", func(t *testing.T) { TestReads(ctx, t, impl, dir+"/reads.txt") })
	t.Run(iName+"_Writes", func(t *testing.T) { TestWrites(ctx, t, impl, dir+"/writes") })
	t.Run(iName+"_Discard", func(t *testing.T) { TestDiscard(ctx, t, impl, dir+"/discard") })
	t.Run(iName+"_Remove", func(t *testing.T) { TestRemove(ctx, t, impl, dir+"/remove.txt") })
	t.Run(iName+"_Stat", func(t *testing.T) { TestStat(ctx, t, impl, dir+"/stat.txt") })
	t.Run(iName+"_List", func(t *testing.T) { TestList(ctx, t, impl, dir+"/match") })
	t.Run(iName+"_ListDir", func(t *testing.T) { TestListDir(ctx, t, impl, dir+"/dirmatch") })
}
