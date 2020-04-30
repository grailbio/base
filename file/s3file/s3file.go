// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package s3file implements grail file interface for S3.
package s3file

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	awsrequest "github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/retry"
)

// Path separator used by s3file.
const pathSeparator = "/"

type maxRetrier interface {
	MaxRetries() int
}

// Options defines options that can be given when creating an s3Impl
type Options struct {
	// ServerSideEncryption allows you to set the `ServerSideEncryption` value to use when
	// uploading files (e.g.  "AES256")
	ServerSideEncryption string
}

type s3Impl struct {
	provider ClientProvider
	options  Options
}

// s3Info implements file.Info interface.
type s3Info struct {
	name    string
	size    int64
	modTime time.Time
	etag    string // = GetObjectOutput.ETag
}

type s3Obj struct {
	obj *s3.Object
	cp  *string
}

type accessMode int

// UploadPartSize is the size of a chunk during multi-part uploads.  It is
// exposed only for unittests.
var UploadPartSize = 16 << 20

const (
	readonly  accessMode = iota // file is opened by Open.
	writeonly                   // file is opened by Create.

	uploadParallelism = 16
)

func mergeFileOpts(opts []file.Opts) (o file.Opts) {
	switch len(opts) {
	case 0:
	case 1:
		o = opts[0]
	default:
		panic(fmt.Sprintf("More than one options specified: %+v", opts))
	}
	return
}

// Operations on a file are internally implemented by a goroutine running
// handleRequests. Requests to handleRequests are sent through s3File.reqCh. The
// response to a request is sent through request.ch.
//
// The user-facing s3File methods, such as Read and Seek are implemented in the following way:
//
// - Create a chan response.
//
// - Send a request object through s3File.ch. The response channel is included
// in the request.  handleRequests() receives the request, handles the request,
// and sends the response.
//
// - Wait for a message from either the response channel or context.Done(),
// whichever comes first.

type requestType int

const (
	seekRequest requestType = iota
	readRequest
	statRequest
	writeRequest
	closeRequest
	abortRequest
)

type request struct {
	ctx     context.Context // context passed to Read, Seek, Close, etc.
	reqType requestType

	// For Read and Write
	buf []byte

	// For Seek
	off    int64
	whence int

	// For sending the response
	ch chan response
}

type response struct {
	n         int     // # of bytes read. Set only by Read.
	off       int64   // Seek location. Set only by Seek.
	info      *s3Info // Set only by Stat.
	signedURL string  // Set only by Presign.
	err       error   // Any error
	uploader  *s3Uploader
}

// s3File implements file.File interface.
type s3File struct {
	name     string         // "s3://bucket/key/.."
	provider ClientProvider // Used to create s3 clients.
	mode     accessMode
	opts     file.Opts

	bucket string // bucket part of "name".
	key    string // key part "name".

	reqCh chan request

	// The following fields are accessed only by the handleRequests thread.
	info *s3Info // File metadata. Filled on demand.

	// Active GetObject body reader. Created by a Read() request. Closed on Seek
	// or Close call.
	bodyReader io.ReadCloser
	// AWS request ID for the bodyReader. Non-empty iff bodyReader!=nil.
	bodyReaderRequestIDs s3RequestIDs

	// Seek offset.
	// INVARIANT: position >= 0 && (position > 0 ⇒ info != nil)
	position int64

	// Used by files opened for writing.
	uploader *s3Uploader
}

type s3Lister struct {
	ctx                         context.Context
	policy                      retryPolicy
	dir, scheme, bucket, prefix string

	object  s3Obj
	objects []s3Obj
	token   *string
	err     error
	done    bool
	recurse bool

	// consecutiveEmptyResponses counts how many times S3's ListObjectsV2WithContext returned
	// 0 records (either contents or common prefixes) consecutively.
	// Many empty responses would cause Scan to appear to hang, so we log a warning.
	consecutiveEmptyResponses int
}

type s3BucketLister struct {
	ctx     context.Context
	policy  retryPolicy
	scheme  string
	err     error
	listed  bool
	bucket  string
	buckets []string
}

// s3Reader implements io.ReadSeeker for S3.
type s3Reader struct {
	ctx context.Context
	f   *s3File
}

// s3Reader implements a placeholder io.Writer for S3.
type s3Writer struct {
	ctx context.Context
	f   *s3File
}

type retryPolicy struct {
	clients       []s3iface.S3API
	policy        retry.Policy
	opts          file.Opts // passed to Open() or Stat request.
	startTime     time.Time // the time requested started.
	retryDeadline time.Time // when to give up retrying.
	retries       int
	waitErr       error // error happened during wait, typically deadline or cancellation.
}

// BackoffPolicy defines backoff timing parameters. It is exposed publicly only
// for unittests.
var BackoffPolicy = retry.Jitter(retry.Backoff(500*time.Millisecond, time.Minute, 1.2), 0.2)

var (
	// MaxRetryDuration defines the max amount of time a request can spend
	// retrying on errors.
	//
	// Requirements:
	//
	// - The value must be >5 minutes. 5 min is the S3 negative-cache TTL.  If
	//   less than 5 minutes, an Open() call w/ RetryWhenNotFound may fail.
	//
	// - It must be long enough to allow CompleteMultiPartUpload to finish after a
	//   retry. The doc says it may take a few minutes even in a successful case.
	MaxRetryDuration = 60 * time.Minute
)

func newRetryPolicy(clients []s3iface.S3API, opts file.Opts) retryPolicy {
	now := time.Now()
	return retryPolicy{
		clients:       clients,
		policy:        BackoffPolicy,
		opts:          opts,
		startTime:     now,
		retryDeadline: now.Add(MaxRetryDuration),
	}
}

type s3RequestIDs struct {
	amzRequestID string
	amzID2       string
}

func (ids s3RequestIDs) String() string {
	return fmt.Sprintf("x-amz-request-id: %s, x-amz-id-2: %s", ids.amzRequestID, ids.amzID2)
}

// This is the same as awsrequest.WithGetResponseHeader, except that it doesn't
// crash when the request fails w/o receiving an HTTP response.
//
// TODO(saito) Revert once awsrequest.WithGetResponseHeaders starts acting more
// gracefully.
func withGetResponseHeaderWithNilCheck(key string, val *string) awsrequest.Option {
	return func(r *awsrequest.Request) {
		r.Handlers.Complete.PushBack(func(req *awsrequest.Request) {
			*val = "(no HTTP response)"
			if req.HTTPResponse != nil && req.HTTPResponse.Header != nil {
				*val = req.HTTPResponse.Header.Get(key)
			}
		})
	}
}

func captureRequestIDs(ptr *s3RequestIDs) awsrequest.Option {
	h0 := withGetResponseHeaderWithNilCheck("x-amz-request-id", &ptr.amzRequestID)
	h1 := withGetResponseHeaderWithNilCheck("x-amz-id-2", &ptr.amzID2)
	return func(r *awsrequest.Request) {
		h0(r)
		h1(r)
	}
}

func getAWSError(err error) (awsError awserr.Error, found bool) {
	errors.Visit(err, func(err error) {
		if err == nil || awsError != nil {
			return
		}
		if e, ok := err.(awserr.Error); ok {
			found = true
			awsError = e
		}
	})
	return
}

// Retriable errors not listed in aws' retry policy.
func otherRetriableError(err error) bool {
	aerr, ok := getAWSError(err)
	if ok && (aerr.Code() == awsrequest.ErrCodeSerialization ||
		aerr.Code() == awsrequest.ErrCodeRead ||
		// The AWS SDK method IsErrorRetryable doesn't consider certain errors as retryable
		// depending on the underlying cause.  (For a detailed explanation as to why,
		// see https://github.com/aws/aws-sdk-go/issues/3027)
		// In our case, we can safely consider every error of type "RequestError" regardless
		// of the underlying cause as a retryable error.
		aerr.Code() == "RequestError" ||
		aerr.Code() == "SlowDown" ||
		aerr.Code() == "InternalError") {
		return true
	}
	if ok && aerr.Code() == "XAmzContentSHA256Mismatch" {
		// Example:
		//
		// XAmzContentSHA256Mismatch: The provided 'x-amz-content-sha256' header
		// does not match what was computed.
		//
		// Happens sporadically for no discernible reason.  Just retry.
		return true
	}
	if ok {
		msg := strings.TrimSpace(aerr.Message())
		if strings.HasSuffix(msg, "amazonaws.com: no such host") {
			// Example:
			//
			// RequestError: send request failed caused by: Get
			// https://grail-patchcnn.s3.us-west-2.amazonaws.com/key: dial tcp: lookup
			// grail-patchcnn.s3.us-west-2.amazonaws.com: no such host
			//
			// This a DNS lookup error on the client side. This may be
			// grail-specific. This error happens after S3 server resolves the bucket
			// successfully, and redirects the client to a backend to fetch data. So
			// accessing a non-existent bucket will not hit this path.
			return true
		}
	}
	if strings.Contains(err.Error(), "resource unavailable") {
		return true
	}
	if strings.Contains(err.Error(), "Service Unavailable") {
		return true
	}
	return false
}

// client returns the s3 client to be use by the caller.
func (r *retryPolicy) client() s3iface.S3API { return r.clients[0] }

// shouldRetry determines if the caller should retry after seeing the given
// error.  It will modify r.clients if it thinks the caller should retry with a
// different client.
func (r *retryPolicy) shouldRetry(ctx context.Context, err error, message string) bool {
	wait := func() bool {
		ctx2, cancel := context.WithDeadline(ctx, r.retryDeadline)
		r.waitErr = retry.Wait(ctx2, r.policy, r.retries)
		cancel()
		if r.waitErr != nil {
			// Context timeout or cancellation
			r.clients = nil
			return false
		}
		r.retries++
		return true
	}

	if err == nil {
		return false
	}
	if awsrequest.IsErrorRetryable(err) || awsrequest.IsErrorThrottle(err) || otherRetriableError(err) {
		// Transient errors. Retry with the same client.
		log.Printf("retry %s: %v", message, err)
		return wait()
	}
	aerr, ok := getAWSError(err)
	if ok {
		if r.opts.RetryWhenNotFound && aerr.Code() == s3.ErrCodeNoSuchKey {
			log.Printf("retry %s (not found): %v", message, err)
			return wait()
		}

		switch aerr.Code() {
		case s3.ErrCodeNoSuchBucket, s3.ErrCodeNoSuchKey:
			// No point in trying again.
			r.clients = nil
			return false
		case "NotFound":
			// GetObject seems to return this error rather ErrCodeNoSuchKey
			r.clients = nil
			return false
		default:
			// Possible cases:
			//
			//- permission errors: we retry using a different client.
			//
			//- non-retriable errors: we retry using a different client, and it will
			// fail again, and we eventually give up. The code it at least correct, if
			// suboptimal.
			//
			// - transient errors we don't yet know. We'll abort when we shouldn't,
			// but there's not much we can do. We'll add these errors to the above
			// case as we discover them.
		}
	}
	if len(r.clients) <= 1 {
		// No more alternate clients to try
		r.clients = nil
		return false
	}
	r.clients = r.clients[1:]
	return true
}

// NewImplementation creates a new file.Implementation for S3. The provider is
// called to create s3 client objects.
func NewImplementation(provider ClientProvider, opts Options) file.Implementation {
	return &s3Impl{provider, opts}
}

// Run handler in a separate goroutine, then wait for either the handler to
// finish, or ctx to be cancelled.
func runRequest(ctx context.Context, handler func() response) response {
	ch := make(chan response)
	go func() {
		ch <- handler()
		close(ch)
	}()
	select {
	case res := <-ch:
		return res
	case <-ctx.Done():
		return response{err: fmt.Errorf("request cancelled")}
	}
}

func (impl *s3Impl) internalOpen(ctx context.Context, path string, mode accessMode, optsList ...file.Opts) (*s3File, error) {
	opts := mergeFileOpts(optsList)
	_, bucket, key, err := ParseURL(path)
	if err != nil {
		return nil, err
	}
	var uploader *s3Uploader
	if mode == writeonly {
		resp := runRequest(ctx, func() response {
			u, err := newUploader(ctx, impl.provider, impl.options, path, bucket, key, opts)
			return response{uploader: u, err: err}
		})
		if resp.err != nil {
			return nil, resp.err
		}
		uploader = resp.uploader
	}
	f := &s3File{
		name:     path,
		mode:     mode,
		opts:     opts,
		provider: impl.provider,
		bucket:   bucket,
		key:      key,
		uploader: uploader,
		reqCh:    make(chan request, 16),
	}
	go f.handleRequests()
	return f, err
}

// Open opens a file for reading. The provided path should be of form
// "bucket/key..."
func (impl *s3Impl) Open(ctx context.Context, path string, opts ...file.Opts) (file.File, error) {
	f, err := impl.internalOpen(ctx, path, readonly, opts...)
	res := f.runRequest(ctx, request{reqType: statRequest})
	if res.err != nil {
		return nil, res.err
	}
	return f, err
}

// Create opens a file for writing.
func (impl *s3Impl) Create(ctx context.Context, path string, opts ...file.Opts) (file.File, error) {
	return impl.internalOpen(ctx, path, writeonly, opts...)
}

// String implements a human-readable description.
func (impl *s3Impl) String() string { return "s3" }

// List implements file.Implementation interface.
func (impl *s3Impl) List(ctx context.Context, dir string, recurse bool) file.Lister {
	scheme, bucket, key, err := ParseURL(dir)
	if err != nil {
		return &s3Lister{ctx: ctx, dir: dir, err: err}
	}
	if bucket == "" {
		if recurse {
			return &s3Lister{ctx: ctx, dir: dir,
				err: fmt.Errorf("list %s: ListBuckets cannot be combined with recurse option", dir)}
		}
		clients, err := impl.provider.Get(ctx, "ListAllMyBuckets", dir)
		if err != nil {
			return &s3Lister{ctx: ctx, dir: dir, err: err}
		}
		return &s3BucketLister{
			ctx:    ctx,
			scheme: scheme,
			policy: newRetryPolicy(clients, file.Opts{}),
		}
	}
	clients, err := impl.provider.Get(ctx, "ListBucket", dir)
	if err != nil {
		return &s3Lister{ctx: ctx, dir: dir, err: err}
	}
	return &s3Lister{
		ctx:     ctx,
		policy:  newRetryPolicy(clients, file.Opts{}),
		dir:     dir,
		scheme:  scheme,
		bucket:  bucket,
		prefix:  key,
		recurse: recurse,
	}
}

// Stat implements file.Implementation interface.
func (impl *s3Impl) Stat(ctx context.Context, path string, opts ...file.Opts) (file.Info, error) {
	resp := runRequest(ctx, func() response {
		_, bucket, key, err := ParseURL(path)
		if err != nil {
			return response{err: err}
		}
		clients, err := impl.provider.Get(ctx, "GetObject", path)
		if err != nil {
			return response{err: err}
		}
		policy := newRetryPolicy(clients, mergeFileOpts(opts))
		for {
			var ids s3RequestIDs
			resp, err := policy.client().HeadObjectWithContext(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			}, captureRequestIDs(&ids))
			if policy.shouldRetry(ctx, err, path) {
				continue
			}
			if err != nil {
				return response{err: annotate(err, ids, &policy, fmt.Sprintf("s3file.stat %s", path))}
			}
			etag, contentLength := aws.StringValue(resp.ETag), aws.Int64Value(resp.ContentLength)
			if etag == "" {
				return response{err: errors.E(errors.NotExist, "s3file.stat", path, "awsrequestID:", ids.String())}
			}
			if contentLength == 0 && strings.HasSuffix(path, "/") {
				// Assume this is a directory marker:
				// https://web.archive.org/web/20190424231712/https://docs.aws.amazon.com/AmazonS3/latest/user-guide/using-folders.html
				return response{err: errors.E(errors.NotExist, "s3file.stat", path, "awsrequestID:", ids.String())}
			}
			return response{info: &s3Info{
				name:    filepath.Base(path),
				size:    contentLength,
				modTime: aws.TimeValue(resp.LastModified),
				etag:    etag,
			}}
		}
	})
	return resp.info, resp.err
}

// Remove implements file.Implementation interface.
func (impl *s3Impl) Remove(ctx context.Context, path string) error {
	resp := runRequest(ctx, func() response {
		_, bucket, key, err := ParseURL(path)
		if err != nil {
			return response{err: err}
		}
		clients, err := impl.provider.Get(ctx, "DeleteObject", path)
		if err != nil {
			return response{err: errors.E(err, "s3file.remove", path)}
		}
		policy := newRetryPolicy(clients, file.Opts{})
		for {
			var ids s3RequestIDs
			_, err = policy.client().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)},
				captureRequestIDs(&ids))
			if policy.shouldRetry(ctx, err, path) {
				continue
			}
			if err != nil {
				err = annotate(err, ids, &policy, "s3file.remove", path)
			}
			return response{err: err}
		}
	})
	return resp.err
}

// Presign implements file.Implementation interface.
func (impl *s3Impl) Presign(ctx context.Context, path, method string, expiry time.Duration) (string, error) {
	resp := runRequest(ctx, func() response {
		_, bucket, key, err := ParseURL(path)
		if err != nil {
			return response{err: err}
		}
		var op string
		var getRequestFn func(client s3iface.S3API) *awsrequest.Request
		switch method {
		case http.MethodGet:
			op = "GetObject"
			getRequestFn = func(client s3iface.S3API) *awsrequest.Request {
				req, _ := client.GetObjectRequest(&s3.GetObjectInput{Bucket: &bucket, Key: &key})
				return req
			}
		case http.MethodPut:
			op = "PutObject"
			getRequestFn = func(client s3iface.S3API) *awsrequest.Request {
				req, _ := client.PutObjectRequest(&s3.PutObjectInput{Bucket: &bucket, Key: &key})
				return req
			}
		case http.MethodDelete:
			op = "DeleteObject"
			getRequestFn = func(client s3iface.S3API) *awsrequest.Request {
				req, _ := client.DeleteObjectRequest(&s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
				return req
			}
		default:
			return response{err: errors.E(errors.NotSupported, "s3file.presign: unsupported http method", method)}
		}
		clients, err := impl.provider.Get(ctx, op, path)
		if err != nil {
			return response{err: err}
		}
		policy := newRetryPolicy(clients, file.Opts{})
		for {
			var ids s3RequestIDs
			req := getRequestFn(policy.client())
			req.ApplyOptions(captureRequestIDs(&ids))
			url, err := req.Presign(expiry)
			if policy.shouldRetry(ctx, err, path) {
				continue
			}
			if err != nil {
				return response{err: annotate(err, ids, &policy, fmt.Sprintf("s3file.presign %s", path))}
			}
			return response{signedURL: url}
		}
	})
	return resp.signedURL, resp.err
}

func maxRetries(clients []s3iface.S3API) int {
	for _, client := range clients {
		if s, ok := client.(maxRetrier); ok && s.MaxRetries() > 0 {
			return s.MaxRetries()
		}
	}
	return defaultMaxRetries
}

func (f *s3File) handleRequests() {
	for req := range f.reqCh {
		switch req.reqType {
		case seekRequest:
			f.handleSeek(req)
		case readRequest:
			f.handleRead(req)
		case statRequest:
			f.handleStat(req)
		case writeRequest:
			f.handleWrite(req)
		case closeRequest:
			f.handleClose(req)
		case abortRequest:
			f.handleAbort(req)
		default:
			panic(fmt.Sprintf("Illegal request: %+v", req))
		}
		close(req.ch)
	}
}

// Name returns the name of the file.
func (f *s3File) Name() string {
	return f.name
}

func (f *s3File) Close(ctx context.Context) error {
	err := f.runRequest(ctx, request{reqType: closeRequest}).err
	close(f.reqCh)
	f.provider = nil
	return err
}

func (f *s3File) Discard(ctx context.Context) {
	if f.mode != writeonly {
		return
	}
	_ = f.runRequest(ctx, request{reqType: abortRequest})
	close(f.reqCh)
	f.provider = nil
}

func (f *s3File) String() string {
	return f.name
}

// Send a request to the handleRequests goroutine and wait for a response. The
// caller must set all the necessary fields in req, except ctx and ch, which are
// filled by this method. On ctx timeout or cancellation, returns a response
// with non-nil err field.
func (f *s3File) runRequest(ctx context.Context, req request) response {
	resCh := make(chan response, 1)
	req.ctx = ctx
	req.ch = resCh
	f.reqCh <- req
	select {
	case res := <-resCh:
		return res
	case <-ctx.Done():
		return response{err: fmt.Errorf("request cancelled")}
	}
}

func (f *s3File) Stat(ctx context.Context) (file.Info, error) {
	if f.mode != readonly {
		return nil, errors.E(errors.NotSupported, f.name, "stat for writeonly file not supported")
	}
	if f.info == nil {
		panic(f)
	}
	return f.info, nil
}

func newInfo(path string, output *s3.GetObjectOutput) *s3Info {
	return &s3Info{
		name:    filepath.Base(path),
		size:    aws.Int64Value(output.ContentLength),
		modTime: aws.TimeValue(output.LastModified),
		etag:    aws.StringValue(output.ETag),
	}
}

func (f *s3File) handleStat(req request) {
	ctx := req.ctx
	clients, err := f.provider.Get(req.ctx, "GetObject", f.name)
	if err != nil {
		req.ch <- response{err: errors.E(err, fmt.Sprintf("s3file.stat %v", f.name))}
		return
	}
	policy := newRetryPolicy(clients, f.opts)
	for {
		var ids s3RequestIDs
		output, err := policy.client().HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.key)},
			captureRequestIDs(&ids))
		if policy.shouldRetry(ctx, err, f.name) {
			continue
		}
		if err != nil {
			req.ch <- response{err: annotate(err, ids, &policy, "s3file.stat", f.name)}
			return
		}
		if aws.StringValue(output.ETag) == "" {
			req.ch <- response{err: errors.E("read", f.name, errors.NotExist, "awsrequestID:", ids.String())}
			return
		}
		f.info = &s3Info{
			name:    filepath.Base(f.name),
			size:    aws.Int64Value(output.ContentLength),
			modTime: aws.TimeValue(output.LastModified),
			etag:    aws.StringValue(output.ETag),
		}
		req.ch <- response{err: nil}
		return
	}
}

func (f *s3File) Reader(ctx context.Context) io.ReadSeeker {
	if f.mode != readonly {
		return file.NewError(fmt.Errorf("reader %v: file is not opened in read mode", f.name))
	}
	return &s3Reader{ctx: ctx, f: f}
}

func (f *s3File) Writer(ctx context.Context) io.Writer {
	if f.mode != writeonly {
		return file.NewError(fmt.Errorf("writer %v: file is not opened in write mode", f.name))
	}
	return &s3Writer{ctx: ctx, f: f}
}

// Seek implements io.Seeker
func (r *s3Reader) Seek(offset int64, whence int) (int64, error) {
	res := r.f.runRequest(r.ctx, request{
		reqType: seekRequest,
		off:     offset,
		whence:  whence,
	})
	return res.off, res.err
}

// Seek implements io.Seeker
func (f *s3File) handleSeek(req request) {
	if f.info == nil {
		panic("stat not filled")
	}
	var newPosition int64
	switch req.whence {
	case io.SeekStart:
		newPosition = req.off
	case io.SeekCurrent:
		newPosition = f.position + req.off
	case io.SeekEnd:
		newPosition = f.info.size + req.off
	default:
		req.ch <- response{off: f.position, err: fmt.Errorf("s3file.seek(%s,%d,%d): illegal whence", f.name, req.off, req.whence)}
		return
	}
	if newPosition < 0 {
		req.ch <- response{off: f.position, err: fmt.Errorf("s3file.seek(%s,%d,%d): out-of-bounds seek", f.name, req.off, req.whence)}
		return
	}
	if newPosition == f.position {
		req.ch <- response{off: f.position}
		return
	}
	f.position = newPosition
	if f.bodyReader != nil {
		f.bodyReader.Close() // nolint: errcheck
		f.bodyReader = nil
		f.bodyReaderRequestIDs = s3RequestIDs{}
	}
	req.ch <- response{off: f.position}
}

// Read implements io.Reader
func (r *s3Reader) Read(p []byte) (n int, err error) {
	res := r.f.runRequest(r.ctx, request{
		reqType: readRequest,
		buf:     p,
	})
	return res.n, res.err
}

func (f *s3File) startGetObjectRequest(ctx context.Context, policy *retryPolicy) error {
	for {
		if f.bodyReader != nil {
			panic("get request still active")
		}
		input := &s3.GetObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.key),
		}
		if f.position > 0 {
			// We either seeked or read before. So f.info must have been set.
			if f.info == nil {
				panic(fmt.Sprintf("read %v: nil info: %+v", f.name, f))
			}
			if f.position >= f.info.size {
				return io.EOF
			}
			input.Range = aws.String(fmt.Sprintf("bytes=%d-", f.position))
		}
		var ids s3RequestIDs
		output, err := policy.client().GetObjectWithContext(ctx, input, captureRequestIDs(&ids))
		if policy.shouldRetry(ctx, err, f.name) {
			continue
		}
		if err != nil {
			return annotate(err, ids, policy)
		}
		if *output.ETag == "" {
			output.Body.Close() // nolint: errcheck
			return fmt.Errorf("read %v: File does not exist, awsrequestID: %v", f.name, ids)
		}
		if f.info != nil && f.info.etag != *output.ETag {
			output.Body.Close() // nolint: errcheck
			return errors.E(
				errors.Precondition,
				fmt.Sprintf("read %v: ETag changed from %v to %v, awsrequestID: %v", f.name, f.info.etag, *output.ETag, ids))
		}
		f.bodyReader = output.Body // take ownership
		f.bodyReaderRequestIDs = ids
		if f.info == nil {
			f.info = newInfo(f.name, output)
		}
		return nil
	}
}

// Read implements io.Reader
func (f *s3File) handleRead(req request) {
	buf := req.buf
	clients, err := f.provider.Get(req.ctx, "GetObject", f.name)
	if err != nil {
		req.ch <- response{err: errors.E(err, fmt.Sprintf("s3file.read %v", f.name))}
		return
	}
	maxRetries := maxRetries(clients)
	policy := newRetryPolicy(clients, f.opts)
	retries := 0 // TODO(saito) use retryPolicy instead
	for len(buf) > 0 {
		if f.bodyReader == nil {
			if err = f.startGetObjectRequest(req.ctx, &policy); err != nil {
				break
			}
		}
		var n int
		n, err = f.bodyReader.Read(buf)
		if n > 0 {
			buf = buf[n:]
			f.position += int64(n)
		}
		if err != nil {
			requestIDs := f.bodyReaderRequestIDs
			f.bodyReader.Close() // nolint: errcheck
			f.bodyReader = nil
			f.bodyReaderRequestIDs = s3RequestIDs{}
			if err != io.EOF {
				retries++
				if retries <= maxRetries {
					log.Error.Printf("s3read %v: retrying (%d) GetObject after error %v, awsrequestID: %v",
						f.name, retries, err, requestIDs)
					continue
				}
			}
			break
		}
	}
	totalBytesRead := len(req.buf) - len(buf)
	if err != nil && err != io.EOF {
		err = errors.E(err, fmt.Sprintf("s3file.read %v", f.name))
	}
	req.ch <- response{n: totalBytesRead, err: err}
}

func (f *s3File) handleWrite(req request) {
	f.uploader.write(req.buf)
	req.ch <- response{n: len(req.buf), err: nil}
}

func (o s3Obj) name() string {
	if o.obj == nil {
		return *o.cp
	}
	return *o.obj.Key
}

type uploadChunk struct {
	client   s3iface.S3API
	uploadID string
	partNum  int64
	buf      *[]byte
}

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
			params, captureRequestIDs(&ids))
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
		resp, err := chunk.client.UploadPartWithContext(u.ctx, params, captureRequestIDs(&ids))
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
		}, captureRequestIDs(&ids))
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
			_, err := u.client.PutObjectWithContext(u.ctx, input, captureRequestIDs(&ids))
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
		_, err := u.client.CompleteMultipartUploadWithContext(u.ctx, params, captureRequestIDs(&ids))
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

func (w *s3Writer) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	res := w.f.runRequest(w.ctx, request{
		reqType: writeRequest,
		buf:     p,
	})
	return res.n, res.err
}

func (f *s3File) handleClose(req request) {
	var err error
	if f.uploader != nil {
		err = f.uploader.finish()
	} else if f.bodyReader != nil {
		if e := f.bodyReader.Close(); e != nil && err == nil {
			err = e
		}
	}
	if err != nil {
		err = errors.E(err, "s3file.close", f.name)
	}
	req.ch <- response{err: err}
}

func (f *s3File) handleAbort(req request) {
	err := f.uploader.abort()
	if err != nil {
		err = errors.E(err, "s3file.abort", f.name)
	}
	req.ch <- response{err: err}
}

func (l *s3BucketLister) Scan() bool {
	if !l.listed {
		for {
			var ids s3RequestIDs
			res, err := l.policy.client().ListBucketsWithContext(l.ctx, &s3.ListBucketsInput{},
				captureRequestIDs(&ids))
			if l.policy.shouldRetry(l.ctx, err, "listbuckets") {
				continue
			}
			if err != nil {
				l.err = annotate(err, ids, &l.policy, "s3file.listbuckets")
				return false
			}
			for _, bucket := range res.Buckets {
				l.buckets = append(l.buckets, *bucket.Name)
			}
			break
		}
		l.listed = true
	}
	if len(l.buckets) == 0 {
		return false
	}
	l.bucket, l.buckets = l.buckets[0], l.buckets[1:]
	return true
}

func (l *s3BucketLister) Path() string {
	return fmt.Sprintf("%s://%s", l.scheme, l.bucket)
}

func (l *s3BucketLister) Info() file.Info { return nil }

func (l *s3BucketLister) IsDir() bool {
	return true
}

// Err returns an error, if any.
func (l *s3BucketLister) Err() error {
	return l.err
}

// Scan implements Lister.Scan
func (l *s3Lister) Scan() bool {
	for {
		if l.err != nil {
			return false
		}
		l.err = l.ctx.Err()
		if l.err != nil {
			return false
		}
		if len(l.objects) > 0 {
			l.object, l.objects = l.objects[0], l.objects[1:]
			ll := len(l.prefix)
			// Ignore keys whose path component isn't exactly equal to l.prefix.  For
			// example, if l.prefix="foo/bar", then we yield "foo/bar" and
			// "foo/bar/baz", but not "foo/barbaz".
			keyVal := l.object.name()
			if ll > 0 && len(keyVal) > ll {
				if l.prefix[ll-1] == '/' {
					// Treat prefix "foo/bar/" as "foo/bar".
					ll--
				}
				if keyVal[ll] != '/' {
					continue
				}
			}
			return true
		}
		if l.done {
			return false
		}

		var prefix string
		if l.showDirs() && !strings.HasSuffix(l.prefix, pathSeparator) && l.prefix != "" {
			prefix = l.prefix + pathSeparator
		} else {
			prefix = l.prefix
		}

		req := &s3.ListObjectsV2Input{
			Bucket:            aws.String(l.bucket),
			ContinuationToken: l.token,
			Prefix:            aws.String(prefix),
		}

		if l.showDirs() {
			req.Delimiter = aws.String(pathSeparator)
		}
		var ids s3RequestIDs
		res, err := l.policy.client().ListObjectsV2WithContext(l.ctx, req, captureRequestIDs(&ids))
		if l.policy.shouldRetry(l.ctx, err, l.dir) {
			continue
		}
		if err != nil {
			l.err = annotate(err, ids, &l.policy, fmt.Sprintf("s3file.list s3://%s/%s", l.bucket, l.prefix))
			return false
		}
		l.token = res.NextContinuationToken
		nRecords := len(res.Contents)
		if l.showDirs() {
			nRecords += len(res.CommonPrefixes)
		}
		if nRecords > 0 {
			l.consecutiveEmptyResponses = 0
		} else {
			l.consecutiveEmptyResponses++
			if n := l.consecutiveEmptyResponses; n > 7 && n&(n-1) == 0 {
				log.Printf("s3file.list.scan: warning: S3 returned empty response %d consecutive times", n)
			}
		}
		l.objects = make([]s3Obj, 0, nRecords)
		for _, objVal := range res.Contents {
			l.objects = append(l.objects, s3Obj{obj: objVal})
		}
		if l.showDirs() { // add the pseudo Dirs
			for _, cpVal := range res.CommonPrefixes {
				// Follow the Linux convention that directories do not come back with a trailing /
				// when read by ListDir.  To determine it is a directory, it is necessary to
				// call implementation.Stat on the path and check IsDir()
				pseudoDirName := *cpVal.Prefix
				if strings.HasSuffix(pseudoDirName, pathSeparator) {
					pseudoDirName = pseudoDirName[:len(pseudoDirName)-1]
				}
				l.objects = append(l.objects, s3Obj{cp: &pseudoDirName})
			}
		}

		l.done = !aws.BoolValue(res.IsTruncated)
	}
}

// Path implements Lister.Path
func (l *s3Lister) Path() string {
	return fmt.Sprintf("%s://%s/%s", l.scheme, l.bucket, l.object.name())
}

// Info implements Lister.Info
func (l *s3Lister) Info() file.Info {
	if obj := l.object.obj; obj != nil {
		return &s3Info{
			size:    *obj.Size,
			modTime: *obj.LastModified,
			etag:    *obj.ETag,
		}
	}
	return nil
}

// IsDir implements Lister.IsDir
func (l *s3Lister) IsDir() bool {
	return l.object.cp != nil
}

// Err returns an error, if any.
func (l *s3Lister) Err() error {
	return l.err
}

// showDirs controls whether CommonPrefixes are returned during a scan
func (l *s3Lister) showDirs() bool {
	return !l.recurse
}

func (i *s3Info) Name() string       { return i.name }
func (i *s3Info) Size() int64        { return i.size }
func (i *s3Info) ModTime() time.Time { return i.modTime }
func (i *s3Info) ETag() string       { return i.etag }

// ParseURL parses a path of form "s3://grail-bucket/dir/file" and returns
// ("s3", "grail-bucket", "dir/file", nil).
func ParseURL(url string) (scheme, bucket, key string, err error) {
	var suffix string
	scheme, suffix, err = file.ParsePath(url)
	if err != nil {
		return "", "", "", err
	}
	parts := strings.SplitN(suffix, pathSeparator, 2)
	if len(parts) == 1 {
		return scheme, parts[0], "", nil
	}
	return scheme, parts[0], parts[1], nil
}

// Annotate interprets err as an AWS request error and returns a version of it
// annotated with severity and kind from the errors package. The optional args
// are passed to errors.E.
func annotate(err error, ids s3RequestIDs, retry *retryPolicy, args ...interface{}) error {
	e := func(prefixArgs ...interface{}) error {
		msgs := append(prefixArgs, args...)
		msgs = append(msgs, "awsrequestID:", ids.String())
		if retry.waitErr != nil {
			msgs = append(msgs, fmt.Sprintf("[waitErr=%v]", retry.waitErr))
		}
		msgs = append(msgs, fmt.Sprintf("[retries=%d, start=%v]", retry.retries, retry.startTime))
		return errors.E(msgs...)
	}
	aerr, ok := getAWSError(err)
	if !ok {
		return e(err)
	}
	if awsrequest.IsErrorThrottle(err) {
		return e(err, errors.Temporary, errors.Unavailable)
	}
	if awsrequest.IsErrorRetryable(err) {
		return e(err, errors.Temporary)
	}
	// The underlying error was an S3 error. Try to classify it.
	// Best guess based on Amazon's descriptions:
	switch aerr.Code() {
	// Code NotFound is not documented, but it's what the API actually returns.
	case s3.ErrCodeNoSuchBucket, s3.ErrCodeNoSuchKey, "NoSuchVersion", "NotFound":
		return e(err, errors.NotExist)
	case "AccessDenied":
		return e(err, errors.NotAllowed)
	case "InvalidRequest", "InvalidArgument", "EntityTooSmall", "EntityTooLarge", "KeyTooLong", "MethodNotAllowed":
		return e(err, errors.Fatal)
	case "ExpiredToken", "AccountProblem", "ServiceUnavailable", "TokenRefreshRequired", "OperationAborted":
		return e(err, errors.Unavailable)
	case "PreconditionFailed":
		return e(err, errors.Precondition)
	case "SlowDown":
		return e(errors.Temporary, errors.Unavailable)
	}
	return e(err)
}
