// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package s3file_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/file/internal/testutil"
	"github.com/grailbio/base/file/s3file"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/s3test"
)

var (
	s3BucketFlag = flag.String("s3-bucket", "", "If set, run a unittest against a real S3 bucket named in this flag")
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

type testProvider struct {
	clients []s3iface.S3API
}

func (p *testProvider) Get(ctx context.Context, op, path string) ([]s3iface.S3API, error) {
	return p.clients, nil
}

func (p *testProvider) NotifyResult(ctx context.Context, op, path string, client s3iface.S3API, err error) {
}

func newClient(t *testing.T) *s3test.Client { return s3test.NewClient(t, "b") }
func permErrorClient(t *testing.T) s3iface.S3API {
	c := s3test.NewClient(t, "b")
	c.Err = errors.New("test permission error")
	return c
}

func TestS3(t *testing.T) {
	provider := &testProvider{clients: []s3iface.S3API{permErrorClient(t), newClient(t)}}
	ctx := context.Background()
	impl := s3file.NewImplementation(provider, s3file.Options{})
	testutil.TestAll(ctx, t, impl, "s3://b/dir")
}

func TestListBucketRoot(t *testing.T) {
	provider := &testProvider{clients: []s3iface.S3API{newClient(t)}}
	ctx := context.Background()
	impl := s3file.NewImplementation(provider, s3file.Options{})

	f, err := impl.Create(ctx, "s3://b/0.txt")
	assert.NoError(t, err)
	_, err = f.Writer(ctx).Write([]byte("data"))
	assert.NoError(t, err)
	assert.NoError(t, f.Close(ctx))

	l := impl.List(ctx, "s3://b", true)
	assert.True(t, l.Scan(), "err: %v", l.Err())
	assert.EQ(t, "s3://b/0.txt", l.Path())
	assert.False(t, l.Scan())
	assert.NoError(t, l.Err())
}

func TestErrors(t *testing.T) {
	provider := &testProvider{clients: []s3iface.S3API{permErrorClient(t)}}
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

func TestRetryAfterError(t *testing.T) {
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
		assert.Regexp(t, err, "Request cancelled")
		assert.Regexp(t, f.Close(ctx), "Request cancelled")
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
