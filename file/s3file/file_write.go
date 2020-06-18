package s3file

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
)

// A helper class for driving s3manager.Uploader through an io.Writer-like
// interface. Its write() method will feed data incrementally to the uploader,
// and finish() will wait for all the uploads to finish.
type s3Uploader struct {
	ctx               context.Context
	client            s3iface.S3API
	path, bucket, key string
	opts              file.Opts
	s3opts            Options
	uploadID          string
	createTime        time.Time // time of file.Create() call
	// curBuf is only accessed by the handleRequests thread.
	curBuf      *[]byte
	nextPartNum int64

	bufPool sync.Pool
	reqCh   chan uploadChunk
	err     errors.Once
	sg      sync.WaitGroup
	mu      sync.Mutex
	parts   []*s3.CompletedPart
}

type uploadChunk struct {
	client   s3iface.S3API
	uploadID string
	partNum  int64
	buf      *[]byte
}

const uploadParallelism = 16

// UploadPartSize is the size of a chunk during multi-part uploads.  It is
// exposed only for unittests.
var UploadPartSize = 16 << 20

func newUploader(ctx context.Context, provider ClientProvider, opts Options, path, bucket, key string, fileOpts file.Opts) (*s3Uploader, error) {
	clients, err := provider.Get(ctx, "PutObject", path)
	if err != nil {
		return nil, errors.E(err, "s3file.write", path)
	}
	params := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	// Add any non-default options
	if opts.ServerSideEncryption != "" {
		params.SetServerSideEncryption(opts.ServerSideEncryption)
	}

	u := &s3Uploader{
		ctx:         ctx,
		path:        path,
		bucket:      bucket,
		key:         key,
		opts:        fileOpts,
		s3opts:      opts,
		createTime:  time.Now(),
		bufPool:     sync.Pool{New: func() interface{} { slice := make([]byte, UploadPartSize); return &slice }},
		nextPartNum: 1,
	}
	policy := newRetryPolicy(clients, file.Opts{})
	for {
		var ids s3RequestIDs
		resp, err := policy.client().CreateMultipartUploadWithContext(ctx,
			params, ids.captureOption())
		if policy.shouldRetry(ctx, err, path) {
			continue
		}
		if err != nil {
			return nil, annotate(err, ids, &policy, "s3file.CreateMultipartUploadWithContext", path)
		}
		u.client = policy.client()
		u.uploadID = *resp.UploadId
		if u.uploadID == "" {
			panic(fmt.Sprintf("empty uploadID: %+v, awsrequestID: %v", resp, ids))
		}
		break
	}

	u.reqCh = make(chan uploadChunk, uploadParallelism)
	for i := 0; i < uploadParallelism; i++ {
		u.sg.Add(1)
		go u.uploadThread()
	}
	return u, nil
}

func (u *s3Uploader) uploadThread() {
	defer u.sg.Done()
	for chunk := range u.reqCh {
		policy := newRetryPolicy([]s3iface.S3API{chunk.client}, file.Opts{})
	retry:
		params := &s3.UploadPartInput{
			Bucket:     aws.String(u.bucket),
			Key:        aws.String(u.key),
			Body:       bytes.NewReader(*chunk.buf),
			UploadId:   aws.String(chunk.uploadID),
			PartNumber: &chunk.partNum,
		}
		var ids s3RequestIDs
		resp, err := chunk.client.UploadPartWithContext(u.ctx, params, ids.captureOption())
		if policy.shouldRetry(u.ctx, err, u.path) {
			goto retry
		}
		u.bufPool.Put(chunk.buf)
		if err != nil {
			u.err.Set(annotate(err, ids, &policy, fmt.Sprintf("s3file.UploadPartWithContext s3://%s/%s", u.bucket, u.key)))
			continue
		}
		partNum := chunk.partNum
		completed := &s3.CompletedPart{ETag: resp.ETag, PartNumber: &partNum}
		u.mu.Lock()
		u.parts = append(u.parts, completed)
		u.mu.Unlock()
	}
}

// write appends data to file. It can be called only by the request thread.
func (u *s3Uploader) write(buf []byte) {
	if len(buf) == 0 {
		panic("empty buf in write")
	}
	for len(buf) > 0 {
		if u.curBuf == nil {
			u.curBuf = u.bufPool.Get().(*[]byte)
			*u.curBuf = (*u.curBuf)[:0]
		}
		if cap(*u.curBuf) != UploadPartSize {
			panic("empty buf")
		}
		uploadBuf := *u.curBuf
		space := uploadBuf[len(uploadBuf):cap(uploadBuf)]
		n := len(buf)
		if n < len(space) {
			copy(space, buf)
			*u.curBuf = uploadBuf[0 : len(uploadBuf)+n]
			return
		}
		copy(space, buf)
		buf = buf[len(space):]
		*u.curBuf = uploadBuf[0:cap(uploadBuf)]
		u.reqCh <- uploadChunk{client: u.client, uploadID: u.uploadID, partNum: u.nextPartNum, buf: u.curBuf}
		u.nextPartNum++
		u.curBuf = nil
	}
}

func (u *s3Uploader) abort() error {
	policy := newRetryPolicy([]s3iface.S3API{u.client}, file.Opts{})
	for {
		var ids s3RequestIDs
		_, err := u.client.AbortMultipartUploadWithContext(u.ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(u.bucket),
			Key:      aws.String(u.key),
			UploadId: aws.String(u.uploadID),
		}, ids.captureOption())
		if !policy.shouldRetry(u.ctx, err, u.path) {
			if err != nil {
				err = annotate(err, ids, &policy, fmt.Sprintf("s3file.AbortMultiPartUploadWithContext s3://%s/%s", u.bucket, u.key))
			}
			return err
		}
	}
}

// finish finishes writing. It can be called only by the request thread.
func (u *s3Uploader) finish() error {
	if u.curBuf != nil && len(*u.curBuf) > 0 {
		u.reqCh <- uploadChunk{client: u.client, uploadID: u.uploadID, partNum: u.nextPartNum, buf: u.curBuf}
		u.curBuf = nil
	}
	close(u.reqCh)
	u.sg.Wait()
	policy := newRetryPolicy([]s3iface.S3API{u.client}, file.Opts{})
	if err := u.err.Err(); err != nil {
		u.abort() // nolint: errcheck
		return err
	}
	if len(u.parts) == 0 {
		// Special case: an empty file. CompleteMultiPartUpload with empty parts causes an error,
		// so work around the bug by issuing a separate PutObject request.
		u.abort() // nolint: errcheck
		for {
			input := &s3.PutObjectInput{
				Bucket: aws.String(u.bucket),
				Key:    aws.String(u.key),
				Body:   bytes.NewReader(nil),
			}
			if u.s3opts.ServerSideEncryption != "" {
				input.SetServerSideEncryption(u.s3opts.ServerSideEncryption)
			}

			var ids s3RequestIDs
			_, err := u.client.PutObjectWithContext(u.ctx, input, ids.captureOption())
			if !policy.shouldRetry(u.ctx, err, u.path) {
				if err != nil {
					err = annotate(err, ids, &policy, fmt.Sprintf("s3file.PutObjectWithContext s3://%s/%s", u.bucket, u.key))
				}
				u.err.Set(err)
				break
			}
		}
		return u.err.Err()
	}
	// Common case. Complete the multi-part upload.
	closeStartTime := time.Now()
	sort.Slice(u.parts, func(i, j int) bool { // Parts must be sorted in PartNumber order.
		return *u.parts[i].PartNumber < *u.parts[j].PartNumber
	})
	params := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(u.bucket),
		Key:             aws.String(u.key),
		UploadId:        aws.String(u.uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: u.parts},
	}
	for {
		var ids s3RequestIDs
		_, err := u.client.CompleteMultipartUploadWithContext(u.ctx, params, ids.captureOption())
		if aerr, ok := getAWSError(err); ok && aerr.Code() == "NoSuchUpload" {
			if u.opts.IgnoreNoSuchUpload {
				// Here we managed to upload >=1 part, so the uploadID must have been
				// valid some point in the past.
				//
				// TODO(saito) we could check that upload isn't too old (say <= 7 days),
				// or that the file actually exists.
				log.Error.Printf("close %s: IgnoreNoSuchUpload is set; ignoring %v %+v", u.path, err, ids)
				err = nil
			}
		}
		if !policy.shouldRetry(u.ctx, err, u.path) {
			if err != nil {
				err = annotate(err, ids, &policy,
					fmt.Sprintf("s3file.CompleteMultipartUploadWithContext s3://%s/%s, "+
						"created at %v, started closing at %v, failed at %v",
						u.bucket, u.key, u.createTime, closeStartTime, time.Now()))
			}
			u.err.Set(err)
			break
		}
	}
	if u.err.Err() != nil {
		u.abort() // nolint: errcheck
	}
	return u.err.Err()
}
