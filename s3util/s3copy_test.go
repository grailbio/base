package s3util

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	"github.com/grailbio/base/retry"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/s3test"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

const testBucket = "test_bucket"

var (
	testKeys  = map[string]*testutil.ByteContent{"test/x": content("some sample content")}
	errorKeys = map[string]error{
		"key_awscanceled":       awserr.New(request.CanceledErrorCode, "test", nil),
		"key_nosuchkey":         awserr.New(s3.ErrCodeNoSuchKey, "test", nil),
		"key_badrequest":        awserr.New("BadRequest", "test", nil),
		"key_canceled":          context.Canceled,
		"key_deadlineexceeded":  context.DeadlineExceeded,
		"key_awsrequesttimeout": awserr.New("RequestTimeout", "test", nil),
		"key_nestedEOFrequest":  awserr.New("MultipartUpload", "test", awserr.New("SerializationError", "test2", fmt.Errorf("unexpected EOF"))),
	}
)

func newTestClient(t *testing.T) *s3test.Client {
	t.Helper()
	client := s3test.NewClient(t, testBucket)
	client.Region = "us-west-2"
	for k, v := range testKeys {
		client.SetFileContentAt(k, v, "")
	}
	return client
}

func newFailingTestClient(t *testing.T, fn *failN) *s3test.Client {
	t.Helper()
	client := newTestClient(t)
	client.Err = func(api string, input interface{}) error {
		switch api {
		case "UploadPartCopyWithContext":
			if upc, ok := input.(*s3.UploadPartCopyInput); ok {
				// Possibly fail the first part with an error based on the key
				if *upc.PartNumber == int64(1) && fn.fail() {
					return errorKeys[*upc.Key]
				}
			}
		case "CopyObjectRequest":
			if req, ok := input.(*s3.CopyObjectInput); ok && fn.fail() {
				return errorKeys[*req.Key]
			}
		}
		return nil
	}
	return client
}

func TestBucketKey(t *testing.T) {
	for _, tc := range []struct {
		url, wantBucket, wantKey string
		wantErr                  bool
	}{
		{"s3://bucket/key", "bucket", "key", false},
		{"s3://some_other-bucket/very/long/key", "some_other-bucket", "very/long/key", false},
	} {
		gotB, gotK, gotE := bucketKey(tc.url)
		if tc.wantErr && gotE == nil {
			t.Errorf("%s got no error, want error", tc.url)
			continue
		}
		if got, want := gotB, tc.wantBucket; got != want {
			t.Errorf("got %s want %s", got, want)
		}
		if got, want := gotK, tc.wantKey; got != want {
			t.Errorf("got %s want %s", got, want)
		}
	}
}

func TestCopy(t *testing.T) {
	client := newTestClient(t)
	copier := NewCopier(client)

	srcKey, srcSize, dstKey := "test/x", testKeys["test/x"].Size(), "test/x_copy"
	srcUrl := fmt.Sprintf("s3://%s/%s", testBucket, srcKey)
	dstUrl := fmt.Sprintf("s3://%s/%s", testBucket, dstKey)

	checkObject(t, client, srcKey, testKeys[srcKey])
	if err := copier.Copy(context.Background(), srcUrl, dstUrl, srcSize, nil); err != nil {
		t.Fatal(err)
	}
	checkObject(t, client, dstKey, testKeys[srcKey])
}

func TestCopyWithRetry(t *testing.T) {
	client := newFailingTestClient(t, &failN{n: 2})
	retrier := retry.MaxRetries(retry.Jitter(retry.Backoff(10*time.Millisecond, 50*time.Millisecond, 2), 0.25), 4)
	copier := NewCopierWithParams(client, retrier, 1<<10, 1<<10, testDebugger{t})

	srcKey, srcSize, dstKey := "test/x", testKeys["test/x"].Size(), "key_awsrequesttimeout"
	srcUrl := fmt.Sprintf("s3://%s/%s", testBucket, srcKey)
	dstUrl := fmt.Sprintf("s3://%s/%s", testBucket, dstKey)

	checkObject(t, client, srcKey, testKeys[srcKey])
	if err := copier.Copy(context.Background(), srcUrl, dstUrl, srcSize, nil); err != nil {
		t.Fatal(err)
	}
	checkObject(t, client, dstKey, testKeys[srcKey])
}

func TestCopyMultipart(t *testing.T) {
	bctx := context.Background()
	for _, tc := range []struct {
		client                 *s3test.Client
		dstKey                 string
		size, limit, partsize  int64
		useShortCtx, cancelCtx bool
		wantErr                bool
	}{
		// 100KiB of data, multi-part limit 50KiB, part size 10KiB
		{newTestClient(t), "dst1", 100 << 10, 50 << 10, 10 << 10, false, false, false},
		// 50KiB of data, multi-part limit 50KiB, part size 10KiB
		{newTestClient(t), "dst2", 50 << 10, 50 << 10, 10 << 10, false, false, false},
		{newTestClient(t), "dst3", 100 << 10, 50 << 10, 10 << 10, true, false, true},
		{newTestClient(t), "dst4", 100 << 10, 50 << 10, 10 << 10, false, true, true},
		{newFailingTestClient(t, &failN{n: 2}), "key_badrequest", 100 << 10, 50 << 10, 10 << 10, false, false, false},
		{newFailingTestClient(t, &failN{n: 2}), "key_deadlineexceeded", 100 << 10, 50 << 10, 10 << 10, false, false, false},
		{newFailingTestClient(t, &failN{n: 2}), "key_awsrequesttimeout", 100 << 10, 50 << 10, 10 << 10, false, false, false},
		{newFailingTestClient(t, &failN{n: 2}), "key_nestedEOFrequest", 100 << 10, 50 << 10, 10 << 10, false, false, false},
		{newFailingTestClient(t, &failN{n: 2}), "key_canceled", 100 << 10, 50 << 10, 10 << 10, false, false, true},
		{newFailingTestClient(t, &failN{n: defaultMaxRetries + 1}), "key_badrequest", 100 << 10, 50 << 10, 10 << 10, false, false, true},
	} {
		client := tc.client
		b := make([]byte, tc.size)
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		srcKey, srcContent := "src", &testutil.ByteContent{Data: b}
		client.SetFileContentAt(srcKey, srcContent, "")
		checkObject(t, client, srcKey, srcContent)

		retrier := retry.MaxRetries(retry.Jitter(retry.Backoff(10*time.Millisecond, 50*time.Millisecond, 2), 0.25), defaultMaxRetries)
		copier := NewCopierWithParams(client, retrier, tc.limit, tc.partsize, testDebugger{t})

		ctx := bctx
		var cancel context.CancelFunc
		if tc.useShortCtx {
			ctx, cancel = context.WithTimeout(bctx, 10*time.Nanosecond)
		} else if tc.cancelCtx {
			ctx, cancel = context.WithCancel(bctx)
			cancel()
		}
		srcUrl := fmt.Sprintf("s3://%s/%s", testBucket, srcKey)
		dstUrl := fmt.Sprintf("s3://%s/%s", testBucket, tc.dstKey)

		err := copier.Copy(ctx, srcUrl, dstUrl, tc.size, nil)
		if cancel != nil {
			cancel()
		}
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s got no error, want error", tc.dstKey)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		checkObject(t, client, tc.dstKey, srcContent)
		if t.Failed() {
			t.Logf("case: %v", tc)
		}
	}
}

func content(s string) *testutil.ByteContent {
	return &testutil.ByteContent{Data: []byte(s)}
}

func checkObject(t *testing.T, client *s3test.Client, key string, c *testutil.ByteContent) {
	t.Helper()
	out, err := client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatal(err)
	}
	p, err := ioutil.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := p, c.Data; !bytes.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// failN returns true n times when fail() is called and then returns false, until its reset.
type failN struct {
	n, i int
}

func (p *failN) fail() bool {
	if p.i < p.n {
		p.i++
		return true
	}
	return false
}

func (p *failN) reset() {
	p.i = 0
}

type testDebugger struct{ *testing.T }

func (d testDebugger) Debugf(format string, args ...interface{}) { d.T.Logf(format, args...) }
