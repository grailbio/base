// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package file_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/s3file"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/assert"
	assert2 "github.com/stretchr/testify/assert"
)

type errFile struct {
	err error
}

func (f *errFile) String() string { return f.err.Error() }

func (f *errFile) Open(ctx context.Context, path string, opts ...file.Opts) (file.File, error) {
	return nil, f.err
}

func (f *errFile) Create(ctx context.Context, path string, opts ...file.Opts) (file.File, error) {
	return nil, f.err
}

func (f *errFile) List(ctx context.Context, dir string, recursive bool) file.Lister {
	return nil
}

func (f *errFile) Stat(ctx context.Context, path string, opts ...file.Opts) (file.Info, error) {
	return nil, f.err
}

func (f *errFile) Remove(ctx context.Context, path string) error {
	return f.err
}

func (f *errFile) Presign(ctx context.Context, path, method string, expiry time.Duration) (string, error) {
	return "", f.err
}

func (f *errFile) Close(ctx context.Context) error {
	return f.err
}

func TestRegistration(t *testing.T) {
	testImpl := &errFile{errors.New("test")}
	file.RegisterImplementation("foo", func() file.Implementation { return testImpl })
	assert.True(t, file.FindImplementation("") != nil)
	assert.True(t, file.FindImplementation("foo") == testImpl)
	assert.True(t, file.FindImplementation("foo2") == nil)
}

func doReadFile(ctx context.Context, path string) string {
	got, err := file.ReadFile(ctx, path)
	if err != nil {
		return err.Error()
	}
	return string(got)
}

func TestReadWriteFile(t *testing.T) {
	tempDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()

	ctx := context.Background()
	path := file.Join(tempDir, "test.txt")
	data := "Hello, olleh"
	assert.NoError(t, file.WriteFile(ctx, path, []byte(data)))
	assert.EQ(t, data, doReadFile(ctx, path))
}

func TestRemoveAllNonexistent(t *testing.T) {
	tempDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()
	assert.NoError(t, file.RemoveAll(ctx, file.Join(tempDir, "baddir")))
}

func TestRemoveAllRegularFile(t *testing.T) {
	tempDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()

	path := file.Join(tempDir, "test.txt")
	data := "Hello, olleh"
	assert.NoError(t, file.WriteFile(ctx, path, []byte(data)))
	assert.EQ(t, data, doReadFile(ctx, path))
	assert.NoError(t, file.RemoveAll(ctx, path))
	assert.Regexp(t, doReadFile(ctx, path), "no such file")
}

func TestRemoveAllRecursive(t *testing.T) {
	tempDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()

	dir := file.Join(tempDir, "d")
	data := "Hello, olleh"
	assert.NoError(t, file.WriteFile(ctx, file.Join(dir, "file.txt"), []byte(data)))
	assert.NoError(t, file.WriteFile(ctx, file.Join(dir, "e/file.txt"), []byte(data)))
	assert.NoError(t, file.RemoveAll(ctx, dir))
	assert.Regexp(t, doReadFile(ctx, file.Join(dir, "file.txt")), "no such file")
	assert.Regexp(t, doReadFile(ctx, file.Join(dir, "e/file.txt")), "no such file")
}

func TestCloseAndReport(t *testing.T) {
	closeMsg := "close [seuozr]"
	returnMsg := "return [mntbnb]"

	// No return error, no close error.
	gotErr := func() (err error) {
		f := errFile{}
		defer file.CloseAndReport(context.Background(), &f, &err)
		return nil
	}()
	assert.NoError(t, gotErr)

	// No return error, close error.
	gotErr = func() (err error) {
		f := errFile{errors.New(closeMsg)}
		defer file.CloseAndReport(context.Background(), &f, &err)
		return nil
	}()
	assert.EQ(t, gotErr.Error(), closeMsg)

	// Return error, no close error.
	gotErr = func() (err error) {
		f := errFile{}
		defer file.CloseAndReport(context.Background(), &f, &err)
		return errors.New(returnMsg)
	}()
	assert.EQ(t, gotErr.Error(), returnMsg)

	// Return error, close error.
	gotErr = func() (err error) {
		f := errFile{errors.New(closeMsg)}
		defer file.CloseAndReport(context.Background(), &f, &err)
		return errors.New(returnMsg)
	}()
	assert2.Contains(t, gotErr.Error(), returnMsg)
	assert2.Contains(t, gotErr.Error(), closeMsg)
}

func ExampleParsePath() {
	parse := func(path string) {
		scheme, suffix, err := file.ParsePath(path)
		if err != nil {
			fmt.Printf("%s 🢥 error %v\n", path, err)
			return
		}
		fmt.Printf("%s 🢥 scheme \"%s\", suffix \"%s\"\n", path, scheme, suffix)
	}
	parse("/tmp/test")
	parse("foo://bar")
	parse("foo:///bar")
	parse("foo:bar")
	parse("/foo:bar")
	// Output:
	// /tmp/test 🢥 scheme "", suffix "/tmp/test"
	// foo://bar 🢥 scheme "foo", suffix "bar"
	// foo:///bar 🢥 scheme "foo", suffix "/bar"
	// foo:bar 🢥 error parsepath foo:bar: a URL must start with 'scheme://'
	// /foo:bar 🢥 scheme "", suffix "/foo:bar"
}

func ExampleBase() {
	fmt.Println(file.Base(""))
	fmt.Println(file.Base("foo1"))
	fmt.Println(file.Base("foo2/"))
	fmt.Println(file.Base("/"))
	fmt.Println(file.Base("s3://"))
	fmt.Println(file.Base("s3://blah1"))
	fmt.Println(file.Base("s3://blah2/"))
	fmt.Println(file.Base("s3://foo/blah3//"))
	// Output:
	// .
	// foo1
	// foo2
	// /
	// s3://
	// blah1
	// blah2
	// blah3
}

func ExampleDir() {
	fmt.Println(file.Dir("foo"))
	fmt.Println(file.Dir("."))
	fmt.Println(file.Dir("/a/b"))
	fmt.Println(file.Dir("a/b"))
	fmt.Println(file.Dir("s3://ab/cd"))
	fmt.Println(file.Dir("s3://ab//cd"))
	fmt.Println(file.Dir("s3://a/b/"))
	fmt.Println(file.Dir("s3://a/b//"))
	fmt.Println(file.Dir("s3://a//b//"))
	fmt.Println(file.Dir("s3://a"))
	// Output:
	// .
	// .
	// /a
	// a
	// s3://ab
	// s3://ab
	// s3://a/b
	// s3://a/b
	// s3://a//b
	// s3://
}

func ExampleJoin() {
	fmt.Println(file.Join())
	fmt.Println(file.Join(""))
	fmt.Println(file.Join("foo", "bar"))
	fmt.Println(file.Join("foo", ""))
	fmt.Println(file.Join("foo", "/bar/"))
	fmt.Println(file.Join(".", "foo:bar"))
	fmt.Println(file.Join("s3://foo"))
	fmt.Println(file.Join("s3://foo", "/bar/"))
	fmt.Println(file.Join("s3://foo", "", "bar"))
	fmt.Println(file.Join("s3://foo", "0"))
	fmt.Println(file.Join("s3://foo", "abc"))
	fmt.Println(file.Join("s3://foo//bar", "/", "/baz"))
	// Output:
	// foo/bar
	// foo
	// foo/bar
	// ./foo:bar
	// s3://foo
	// s3://foo/bar
	// s3://foo/bar
	// s3://foo/0
	// s3://foo/abc
	// s3://foo//bar/baz
}

func ExampleIsAbs() {
	fmt.Println(file.IsAbs("foo"))
	fmt.Println(file.IsAbs("/foo"))
	fmt.Println(file.IsAbs("s3://foo"))
	// Output:
	// false
	// true
	// true
}

var once = sync.Once{}

func initBenchmark() {
	once.Do(func() {
		file.RegisterImplementation("s3",
			func() file.Implementation {
				return s3file.NewImplementation(s3file.NewDefaultProvider(session.Options{}), s3file.Options{})
			})
	})
}

var (
	writeFlag  = flag.String("write", "", "Path of the file used by write benchmark.")
	sizeFlag   = flag.Int64("size", 16<<20, "# of bytes to write during benchmark.")
	verifyFlag = flag.Bool("verify", true, "Verify contents of the file created by the write benchmark")
)

func BenchmarkWrite(b *testing.B) {
	initBenchmark()
	if *writeFlag == "" {
		b.Skip("--write flag not set")
	}

	// buf := make([]byte, 64<<10)
	ctx := context.Background()
	b.Logf("Writing %d bytes to %s", *sizeFlag, *writeFlag)
	buf := make([]byte, 64<<10)
	for i := 0; i < b.N; i++ {
		rnd := rand.New(rand.NewSource(0))
		f, err := file.Create(ctx, *writeFlag)
		assert.NoError(b, err)
		w := f.Writer(ctx)

		total := int64(0)
		for total < *sizeFlag {
			b.StopTimer()
			_, err := io.ReadFull(rnd, buf)
			assert.NoError(b, err)
			b.StartTimer()

			n, err := w.Write(buf)
			assert.NoError(b, err)
			assert.EQ(b, n, len(buf))
			total += int64(n)
		}
		assert.NoError(b, f.Close(ctx))
	}

	if *verifyFlag {
		rnd := rand.New(rand.NewSource(0))
		b.StopTimer()
		f, err := file.Open(ctx, *writeFlag)
		assert.NoError(b, err)
		r := f.Reader(ctx)
		total := int64(0)
		expected := make([]byte, 64<<10)
		got := make([]byte, 64<<10)

		for total < *sizeFlag {
			_, err := io.ReadFull(rnd, expected)
			assert.NoError(b, err)
			_, err = io.ReadFull(r, got)
			assert.NoError(b, err)
			assert.EQ(b, expected, got)
			total += int64(len(got))
		}
		assert.NoError(b, f.Close(ctx))
		b.StartTimer()
	}
}
