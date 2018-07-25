// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package digest_test

import (
	"context"
	"crypto"
	_ "crypto/sha256" // Required for the SHA256 constant.
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/s3test"
)

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func TestDigestReader(t *testing.T) {
	digester := digest.Digester(crypto.SHA256)

	dataSize := int64(950)
	segmentSize := int64(100)

	for _, test := range []struct {
		reader io.Reader
		order  []int64
	}{
		{
			&testutil.FakeContentAt{t, dataSize, 0, 0},
			[]int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			&testutil.FakeContentAt{t, dataSize, 0, 0},
			[]int64{1, 0, 3, 2, 5, 4, 7, 6, 9, 8},
		},
		{
			&testutil.FakeContentAt{t, dataSize, 0, 0},
			[]int64{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
		},
	} {
		dra := digester.NewReader(test.reader)
		readerAt, ok := dra.(io.ReaderAt)
		if !ok {
			t.Fatal("reader does not support ReaderAt")
		}

		wg := sync.WaitGroup{}
		wg.Add(len(test.order))
		for _, i := range test.order {
			go func(index int64) {
				defer wg.Done()

				size := min(segmentSize, (dataSize-index)*segmentSize)
				d := make([]byte, size)
				_, err := readerAt.ReadAt(d, index*int64(segmentSize))
				if err != nil {
					t.Fatal(err)
				}
			}(i)
			time.Sleep(10 * time.Millisecond)
		}
		wg.Wait()

		actual, err := dra.Digest()
		if err != nil {
			t.Fatal(err)
		}

		writer := digester.NewWriter()
		content := &testutil.FakeContentAt{t, dataSize, 0, 0}
		if _, err := io.Copy(writer, content); err != nil {
			t.Fatal(err)
		}
		expected := writer.Digest()

		if actual != expected {
			t.Fatalf("digest mismatch: %s vs %s", actual, expected)
		}
	}
}

func TestDigestWriter(t *testing.T) {
	td, err := ioutil.TempDir("", "grail_cache_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(td)

	digester := digest.Digester(crypto.SHA256)

	tests := [][]int{
		{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
	}
	for i := 0; i < 50; i++ {
		tests = append(tests, rand.Perm(10))
	}

	for _, test := range tests {
		testFile := path.Join(td, "testfile")

		output, err := os.Create(testFile)
		if err != nil {
			t.Fatal(err)
		}

		dwa := digester.NewWriterAt(context.Background(), output)

		wg := sync.WaitGroup{}
		wg.Add(len(test))
		for _, i := range test {
			segmentString := strings.Repeat(fmt.Sprintf("%c", 'a'+i), 100)
			offset := int64(i * len(segmentString))

			go func() {
				_, err := dwa.WriteAt([]byte(segmentString), offset)
				if err != nil {
					t.Fatal(err)
				}
				wg.Done()
			}()
			time.Sleep(5 * time.Millisecond)
		}
		wg.Wait()
		output.Close()

		expected, err := dwa.Digest()
		if err != nil {
			t.Fatal(err)
		}

		input, err := os.Open(testFile)
		if err != nil {
			t.Fatal(err)
		}

		w := digester.NewWriter()
		io.Copy(w, input)

		if got := w.Digest(); expected != got {
			t.Fatalf("expected: %v, got: %v", expected, got)
		}

		input.Close()
	}
}

func TestDigestWriterContext(t *testing.T) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	digester := digest.Digester(crypto.SHA256)
	ctx, cancel := context.WithCancel(context.Background())
	w := digester.NewWriterAt(ctx, f)
	_, err = w.WriteAt([]byte{1, 2, 3}, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.WriteAt([]byte{4, 5, 6}, 3)
	if err != nil {
		t.Fatal(err)
	}
	// By now we know the looper is up and running.
	var wg sync.WaitGroup
	wg.Add(10)
	for i := int64(0); i < 10; i++ {
		go func(i int64) {
			_, err := w.WriteAt([]byte{1}, 100+i)
			if got, want := err, ctx.Err(); got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			wg.Done()
		}(i)
	}
	cancel()
	wg.Wait()
}

func TestS3ManagerUpload(t *testing.T) {
	client := s3test.NewClient(t, "test-bucket")

	size := int64(93384620) // Completely random number.

	digester := digest.Digester(crypto.SHA256)
	contentAt := &testutil.FakeContentAt{t, size, 0, 0}
	client.SetFileContentAt("test/test/test", contentAt, "fakesha")
	reader := digester.NewReader(contentAt)

	// The relationship between size and PartSize is:
	//    - size/PartSize > 10 to utilize multiple parallel uploads.
	//    - size%PartSize != 0 so the last part is a partial upload.
	//    - size is small enough that the unittest runs quickly.
	//    - PartSize is the minimum allowed.
	uploader := s3manager.NewUploaderWithClient(client, func(d *s3manager.Uploader) { d.Concurrency = 30; d.PartSize = 5242880 })

	input := &s3manager.UploadInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test/test/test"),
		Body:   reader,
	}

	// Perform an upload.
	_, err := uploader.UploadWithContext(context.Background(), input, func(*s3manager.Uploader) {})
	if err != nil {
		t.Fatal(err)
	}

	got, err := reader.Digest()
	if err != nil {
		t.Fatal(err)
	}

	dw := digester.NewWriter()
	content := &testutil.FakeContentAt{t, size, 0, 0}
	if _, err := io.Copy(dw, content); err != nil {
		t.Fatal(err)
	}
	expected := dw.Digest()

	if got != expected {
		t.Fatalf("digest mismatch, expected %s, got %s", expected, got)
	}
}

func TestS3ManagerDownload(t *testing.T) {
	client := s3test.NewClient(t, "test-bucket")
	client.NumMaxRetries = 10

	size := int64(86738922) // Completely random number.

	digester := digest.Digester(crypto.SHA256)
	contentAt := &testutil.FakeContentAt{t, size, 0, 0.001}
	client.SetFileContentAt("test/test/test", contentAt, "fakesha")
	writer := digester.NewWriterAt(context.Background(), contentAt)

	// The relationship between size and PartSize is:
	//    - size/PartSize > 10 to utilize multiple parallel uploads.
	//    - size%PartSize != 0 so the last part is a partial upload.
	//    - size is small enough that the unittest runs quickly.
	//    - PartSize is the minimum allowed.
	downloader := s3manager.NewDownloaderWithClient(client, func(d *s3manager.Downloader) { d.Concurrency = 30; d.PartSize = 55242880 })

	params := &s3.GetObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test/test/test"),
	}
	_, err := downloader.DownloadWithContext(
		context.Background(),
		writer,
		params,
	)
	if err != nil {
		t.Fatal(err)
	}

	got, err := writer.Digest()
	if err != nil {
		t.Fatal(err)
	}

	dw := digester.NewWriter()
	content := &testutil.FakeContentAt{t, size, 0, 0}
	if _, err := io.Copy(dw, content); err != nil {
		t.Fatal(err)
	}
	expected := dw.Digest()

	if got != expected {
		t.Fatalf("digest mismatch, expected %s, got %s", expected, got)
	}
}
