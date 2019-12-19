// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//+build !unit

package s3file_test

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/internal/testutil"
	"github.com/grailbio/base/file/s3file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/s3test"
)

var (
	s3BucketFlag = flag.String("s3-bucket", "", "If set, run a unittest against a real S3 bucket named in this flag")
	s3DirFlag    = flag.String("s3-dir", "", "S3 directory under -s3-bucket used by some unittests")
	profileFlag  = flag.String("profile", "default", "If set, use the named profile in ~/.aws")
)

type failingContentAt struct {
	prob    float64 // probability of failing requests
	rand    *rand.Rand
	content []byte
}

func doReadAt(src []byte, off64 int64, dest []byte) (int, error) {
	off := int(off64)
	remaining := len(src) - off
	if remaining <= 0 {
		return 0, io.EOF
	}
	if len(dest) < remaining {
		remaining = len(dest)
	}
	copy(dest, src[off:])
	return remaining, nil
}

func doWriteAt(src []byte, off64 int64, dest *[]byte) (int, error) {
	off := int(off64)
	if len(*dest) < off+len(src) {
		tmp := make([]byte, off+len(src))
		copy(tmp, *dest)
		*dest = tmp
	}
	copy((*dest)[off:], src)
	return len(src), nil
}

func (c *failingContentAt) ReadAt(p []byte, off64 int64) (int, error) {
	if p := c.rand.Float64(); p < c.prob {
		return 0, fmt.Errorf("failingContentAt synthetic error")
	}
	n := len(p)
	if n > 1 {
		n = 1 + c.rand.Intn(n-1)
	}
	return doReadAt(c.content, off64, p[:n])
}

func (c *failingContentAt) WriteAt(p []byte, off64 int64) (int, error) {
	return doWriteAt(p, off64, &c.content)
}

func (c *failingContentAt) Size() int64 {
	return int64(len(c.content))
}

func (c *failingContentAt) Checksum() string {
	return fmt.Sprintf("%x", md5.Sum(c.content))
}

type pausingContentAt struct {
	ready   chan bool
	content []byte
}

// ReadAt implements io.ReaderAt.
func (c *pausingContentAt) ReadAt(p []byte, off64 int64) (int, error) {
	<-c.ready
	return doReadAt(c.content, off64, p)
}

// WriteAt implements io.WriterAt
func (c *pausingContentAt) WriteAt(p []byte, off64 int64) (int, error) {
	return doWriteAt(p, off64, &c.content)
}

// Size returns the size of the fake content.
func (c *pausingContentAt) Size() int64 {
	return int64(len(c.content))
}

func (c *pausingContentAt) Checksum() string {
	return fmt.Sprintf("%x", md5.Sum(c.content))
}

type testProvider struct {
	clients []s3iface.S3API
}

func (p *testProvider) Get(ctx context.Context, op, path string) ([]s3iface.S3API, error) {
	return p.clients, nil
}

func (p *testProvider) NotifyResult(ctx context.Context, op, path string, client s3iface.S3API, err error) {
}

func newClient(t *testing.T) *s3test.Client { return s3test.NewClient(t, "b") }
func errorClient(t *testing.T, err error) s3iface.S3API {
	c := s3test.NewClient(t, "b")
	c.Err = func(api string, input interface{}) error {
		return err
	}
	return c
}

func TestS3(t *testing.T) {
	provider := &testProvider{clients: []s3iface.S3API{errorClient(t,
		awserr.New("", // TODO(swami): Use an AWS error code that represents a permission error.
			fmt.Sprintf("test permission error: %s", string(debug.Stack())), nil)), newClient(t)}}
	ctx := context.Background()
	impl := s3file.NewImplementation(provider, s3file.Options{})
	testutil.TestAll(ctx, t, impl, "s3://b/dir")
}

func TestS3WithRetries(t *testing.T) {
	oldPolicy := s3file.BackoffPolicy
	s3file.BackoffPolicy = retry.Backoff(0, 0, 1.0)
	defer func() {
		s3file.BackoffPolicy = oldPolicy
	}()

	ctx := context.Background()
	for iter := 0; iter < 50; iter++ {
		r := rand.New(rand.NewSource(int64(iter)))
		client := newClient(t)
		client.Err = func(api string, input interface{}) error {
			switch r.Intn(6) {
			case 0:
				return awserr.New(request.ErrCodeSerialization, fmt.Sprintf("test failure %s (%s)", api, string(debug.Stack())), nil)
			case 1:
				return awserr.New("RequestError", "send request failed", readConnResetError{})
			}
			return nil
		}
		provider := &testProvider{clients: []s3iface.S3API{client}}
		impl := s3file.NewImplementation(provider, s3file.Options{})
		testutil.TestAll(ctx, t, impl, "s3://b/dir")
	}
}

// WriteFile creates a file with the given contents. Path should be of form
// s3://bucket/key.
func writeFile(ctx context.Context, t *testing.T, impl file.Implementation, path, data string) {
	f, err := impl.Create(ctx, path)
	assert.NoError(t, err)
	_, err = f.Writer(ctx).Write([]byte(data))
	assert.NoError(t, err)
	assert.NoError(t, f.Close(ctx))
}
func TestListBucketRoot(t *testing.T) {
	provider := &testProvider{clients: []s3iface.S3API{newClient(t)}}
	ctx := context.Background()
	impl := s3file.NewImplementation(provider, s3file.Options{})
	writeFile(ctx, t, impl, "s3://b/0.txt", "data")

	l := impl.List(ctx, "s3://b", true)
	assert.True(t, l.Scan(), "err: %v", l.Err())
	assert.EQ(t, "s3://b/0.txt", l.Path())
	assert.False(t, l.Scan())
	assert.NoError(t, l.Err())
}

type readConnResetError struct{}

func (c readConnResetError) Temporary() bool { return false }
func (c readConnResetError) Error() string   { return "read: connection reset" }

func TestErrors(t *testing.T) {
	provider := &testProvider{clients: []s3iface.S3API{errorClient(t,
		awserr.New("", // TODO(swami): Use an AWS error code that represents a permission error.
			fmt.Sprintf("test permission error: %s", string(debug.Stack())), nil))}}
	ctx := context.Background()
	impl := s3file.NewImplementation(provider, s3file.Options{})

	_, err := impl.Create(ctx, "s3://b/junk0.txt")
	assert.Regexp(t, err, "test permission error")

	_, err = impl.Stat(ctx, "s3://b/junk0.txt")
	assert.Regexp(t, err, "test permission error")

	l := impl.List(ctx, "s3://b/foo", true)
	assert.False(t, l.Scan())
	assert.Regexp(t, l.Err(), "test permission error")
}

func TestTransientErrors(t *testing.T) {
	provider := &testProvider{clients: []s3iface.S3API{errorClient(t, awserr.New("RequestError", "send request failed", readConnResetError{}))}}
	impl := s3file.NewImplementation(provider, s3file.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := impl.Stat(ctx, "s3://b/junk0.txt")
	assert.Regexp(t, err, "request cancelled")

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = impl.Stat(ctx, "s3://b/junk0.txt")
	assert.Regexp(t, err, "ran out of time while waiting")
}

func TestWriteRetryAfterError(t *testing.T) {
	client := newClient(t)
	provider := &testProvider{clients: []s3iface.S3API{client}}
	impl := s3file.NewImplementation(provider, s3file.Options{})
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		r := rand.New(rand.NewSource(0))
		client.Err = func(api string, input interface{}) error {
			if r.Intn(3) == 0 {
				fmt.Printf("write: api %s\n", api)
				return awserr.New(request.ErrCodeSerialization, "test failure", nil)
			}
			return nil
		}
		writeFile(ctx, t, impl, "s3://b/0.txt", "data")
	}
}

func TestReadRetryAfterError(t *testing.T) {
	client := newClient(t)
	setContent := func(path string, prob float64, data string) {
		c := &failingContentAt{
			prob:    prob,
			rand:    rand.New(rand.NewSource(0)),
			content: []byte(data),
		}
		checksum := sha256.Sum256(c.content)
		client.SetFileContentAt(path, c, fmt.Sprintf("%x", checksum[:]))
	}

	var contents string
	{
		l := []string{}
		for i := 0; i < 1000; i++ {
			l = append(l, fmt.Sprintf("D%d", i))
		}
		contents = strings.Join(l, ",")
	}

	provider := &testProvider{clients: []s3iface.S3API{client}}
	impl := s3file.NewImplementation(provider, s3file.Options{})
	ctx := context.Background()

	setContent("junk0.txt", 0.3, contents)
	for i := 0; i < 10; i++ {
		client.NumMaxRetries = math.MaxInt32
		f, err := impl.Open(ctx, "b/junk0.txt")
		assert.NoError(t, err)
		r := f.Reader(ctx)
		data, err := ioutil.ReadAll(r)
		assert.NoError(t, err)
		assert.EQ(t, contents, string(data))
		assert.NoError(t, f.Close(ctx))
	}

	setContent("junk1.txt", 1.0 /*fail everything*/, contents)
	{
		client.NumMaxRetries = 10
		f, err := impl.Open(ctx, "b/junk1.txt")
		assert.NoError(t, err)
		r := f.Reader(ctx)
		_, err = ioutil.ReadAll(r)
		assert.Regexp(t, err, "failingContentAt synthetic error")
		assert.NoError(t, f.Close(ctx))
	}
}

func TestRetryWhenNotFound(t *testing.T) {
	client := s3test.NewClient(t, "b")

	provider := &testProvider{clients: []s3iface.S3API{client}}
	impl := s3file.NewImplementation(provider, s3file.Options{})

	ctx := context.Background()
	// By default, there is no retry.
	_, err := impl.Open(ctx, "s3://b/file.txt")
	assert.Regexp(t, err, "NoSuchKey")

	doneCh := make(chan bool)
	go func() {
		_, err := impl.Open(ctx, "s3://b/file.txt", file.Opts{RetryWhenNotFound: true})
		assert.NoError(t, err)
		doneCh <- true
	}()
	time.Sleep(1 * time.Second)
	select {
	case <-doneCh:
		t.Fatal("should not reach here")
	default:
	}
	writeFile(ctx, t, impl, "s3://b/file.txt", "data")
	fmt.Println("wrote file")
	<-doneCh
}

func TestCancellation(t *testing.T) {
	client := s3test.NewClient(t, "b")

	setContent := func(path, data string) *pausingContentAt {
		c := &pausingContentAt{ready: make(chan bool, 1), content: []byte(data)}
		checksum := sha256.Sum256(c.content)
		client.SetFileContentAt(path, c, fmt.Sprintf("%x", checksum[:]))
		return c
	}
	c0 := setContent("test0.txt", "hello")
	_ = setContent("test1.txt", "goodbye")

	provider := &testProvider{clients: []s3iface.S3API{client}}
	impl := s3file.NewImplementation(provider, s3file.Options{})
	{
		c0.ready <- true
		// Reading c0 completes immediately.
		ctx := context.Background()
		f, err := impl.Open(ctx, "s3://b/test0.txt")
		assert.NoError(t, err)
		r := f.Reader(ctx)
		data, err := ioutil.ReadAll(r)
		assert.NoError(t, err)
		assert.EQ(t, "hello", string(data))
		assert.NoError(t, f.Close(ctx))
	}
	{
		// Reading c1 will block.
		f, err := impl.Open(context.Background(), "s3://b/test1.txt")
		assert.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r := f.Reader(ctx)
		_, err = ioutil.ReadAll(r)
		assert.Regexp(t, err, "request cancelled")
		assert.Regexp(t, f.Close(ctx), "request cancelled")
	}
}

func testOverwriteWhileReading(t *testing.T, impl file.Implementation, pathPrefix string) {
	ctx := context.Background()
	path := pathPrefix + "/test.txt"
	writeFile(ctx, t, impl, path, "test0")
	f, err := impl.Open(ctx, path)
	assert.NoError(t, err)

	r := f.Reader(ctx)
	data, err := ioutil.ReadAll(r)
	assert.NoError(t, err)
	assert.EQ(t, "test0", string(data))

	_, err = r.Seek(0, io.SeekStart)
	assert.NoError(t, err)

	writeFile(ctx, t, impl, path, "test0")

	data, err = ioutil.ReadAll(r)
	assert.NoError(t, err)
	assert.EQ(t, "test0", string(data))

	_, err = r.Seek(0, io.SeekStart)
	assert.NoError(t, err)
	writeFile(ctx, t, impl, path, "test1")
	_, err = ioutil.ReadAll(r)
	assert.True(t, errors.Is(errors.Precondition, err), "err=%v", err)
}

func TestWriteLargeFile(t *testing.T) {
	// Reduce the upload chunk size to issue concurrent upload requests to S3.
	oldUploadPartSize := s3file.UploadPartSize
	s3file.UploadPartSize = 128
	defer func() {
		s3file.UploadPartSize = oldUploadPartSize
	}()

	ctx := context.Background()
	provider := &testProvider{clients: []s3iface.S3API{s3test.NewClient(t, "b")}}
	impl := s3file.NewImplementation(provider, s3file.Options{})
	path := "s3://b/test.txt"
	f, err := impl.Create(ctx, path)
	assert.NoError(t, err)
	r := rand.New(rand.NewSource(0))
	var want []byte
	const iters = 400
	for i := 0; i < iters; i++ {
		n := r.Intn(1024) + 100
		data := make([]byte, n)
		n, err := r.Read(data)
		assert.EQ(t, n, len(data))
		assert.NoError(t, err)
		n, err = f.Writer(ctx).Write(data)
		assert.EQ(t, n, len(data))
		assert.NoError(t, err)
		want = append(want, data...)
	}
	assert.NoError(t, f.Close(ctx))

	// Read the file back and verify contents.
	f, err = impl.Open(ctx, path)
	assert.NoError(t, err)
	got := make([]byte, len(want))
	n, _ := f.Reader(ctx).Read(got)
	assert.EQ(t, n, len(want))
	assert.EQ(t, got, want)
	assert.NoError(t, f.Close(ctx))
}

func TestOverwriteWhileReading(t *testing.T) {
	provider := &testProvider{clients: []s3iface.S3API{s3test.NewClient(t, "b")}}
	impl := s3file.NewImplementation(provider, s3file.Options{})
	testOverwriteWhileReading(t, impl, "s3://b/test")
}

func TestNotExist(t *testing.T) {
	provider := &testProvider{clients: []s3iface.S3API{s3test.NewClient(t, "b")}}
	impl := s3file.NewImplementation(provider, s3file.Options{})
	ctx := context.Background()
	// The s3test client fails tests for requests that attempt to
	// access buckets other than the one specified, so we can
	// test only missing keys here.
	_, err := impl.Open(ctx, "b/notexist")
	assert.True(t, errors.Is(errors.NotExist, err))
}

func TestOverwriteWhileReadingAWS(t *testing.T) {
	if *s3BucketFlag == "" {
		t.Skip("Skipping. Set -s3-bucket to run the test.")
	}
	provider := s3file.NewDefaultProvider(session.Options{Profile: *profileFlag})
	impl := s3file.NewImplementation(provider, s3file.Options{})
	testOverwriteWhileReading(t, impl, fmt.Sprintf("s3://%s/tmp/testoverwrite", *s3BucketFlag))
}

func TestPresignRequestsAWS(t *testing.T) {
	if *s3BucketFlag == "" {
		t.Skip("Skipping. Set -s3-bucket to run the test.")
	}
	provider := s3file.NewDefaultProvider(session.Options{Profile: *profileFlag})
	impl := s3file.NewImplementation(provider, s3file.Options{})
	ctx := context.Background()
	const content = "file for testing presigned URLs\n"
	path := fmt.Sprintf("s3://%s/tmp/testpresigned", *s3BucketFlag)

	// Write the dummy file.
	url, err := impl.Presign(ctx, path, "PUT", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Read the dummy file.
	url, err = impl.Presign(ctx, path, "GET", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if content != string(respBytes) {
		t.Errorf("got: %q, want: %q", string(respBytes), content)
	}

	// Delete the dummy file.
	url, err = impl.Presign(ctx, path, "DELETE", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req, err = http.NewRequest(http.MethodDelete, url, strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if _, err := impl.Stat(ctx, path); !errors.Is(errors.NotExist, err) {
		t.Errorf("got: %v\nwant an error of kind NotExist", err)
	}
}

func TestAWS(t *testing.T) {
	if *s3BucketFlag == "" {
		t.Skip("Skipping. Set -s3-bucket to run the test.")
	}
	provider := s3file.NewDefaultProvider(session.Options{Profile: *profileFlag})
	ctx := context.Background()
	impl := s3file.NewImplementation(provider, s3file.Options{})
	testutil.TestAll(ctx, t, impl, "s3://"+*s3BucketFlag+"/tmp")
}

func TestConcurrentUploads(t *testing.T) {
	if *s3BucketFlag == "" || *s3DirFlag == "" {
		t.Skip("Skipping. Set -s3-bucket and -s3-dir to run the test.")
	}
	provider := s3file.NewDefaultProvider(session.Options{Profile: *profileFlag})
	impl := s3file.NewImplementation(provider, s3file.Options{})

	path := fmt.Sprintf("s3://%s/%s/test.txt", *s3BucketFlag, *s3DirFlag)
	ctx := context.Background()

	upload := func() {
		f, err := impl.Create(ctx, path, file.Opts{IgnoreNoSuchUpload: true})
		if err != nil {
			log.Panic(err)
		}
		_, err = f.Writer(ctx).Write([]byte("hello"))
		if err != nil {
			log.Panic(err)
		}
		if err := f.Close(ctx); err != nil {
			log.Panic(err)
		}
	}

	wg := sync.WaitGroup{}
	n := uint64(0)
	for i := 0; i < 4000; i++ {
		wg.Add(1)
		go func() {
			upload()
			if x := atomic.AddUint64(&n, 1); x%100 == 0 {
				log.Printf("%d done", x)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func ExampleParseURL() {
	scheme, bucket, key, err := s3file.ParseURL("s3://grail-bucket/dir/file")
	fmt.Printf("scheme: %s, bucket: %s, key: %s, err: %v\n", scheme, bucket, key, err)
	scheme, bucket, key, err = s3file.ParseURL("s3://grail-bucket/dir/")
	fmt.Printf("scheme: %s, bucket: %s, key: %s, err: %v\n", scheme, bucket, key, err)
	scheme, bucket, key, err = s3file.ParseURL("s3://grail-bucket")
	fmt.Printf("scheme: %s, bucket: %s, key: %s, err: %v\n", scheme, bucket, key, err)
	// Output:
	// scheme: s3, bucket: grail-bucket, key: dir/file, err: <nil>
	// scheme: s3, bucket: grail-bucket, key: dir/, err: <nil>
	// scheme: s3, bucket: grail-bucket, key: , err: <nil>
}
