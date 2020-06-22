package s3file

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
)

// s3File implements file.File interface.
//
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

// Name returns the name of the file.
func (f *s3File) Name() string {
	return f.name
}

func (f *s3File) String() string {
	return f.name
}

// s3Info implements file.Info interface.
type s3Info struct {
	name    string
	size    int64
	modTime time.Time
	etag    string // = GetObjectOutput.ETag
}

func (i *s3Info) Name() string       { return i.name }
func (i *s3Info) Size() int64        { return i.size }
func (i *s3Info) ModTime() time.Time { return i.modTime }
func (i *s3Info) ETag() string       { return i.etag }

func (f *s3File) Stat(ctx context.Context) (file.Info, error) {
	if f.mode != readonly {
		return nil, errors.E(errors.NotSupported, f.name, "stat for writeonly file not supported")
	}
	if f.info == nil {
		panic(f)
	}
	return f.info, nil
}

// s3Reader implements io.ReadSeeker for S3.
type s3Reader struct {
	ctx context.Context
	f   *s3File
}

// Read implements io.Reader
func (r *s3Reader) Read(p []byte) (n int, err error) {
	res := r.f.runRequest(r.ctx, request{
		reqType: readRequest,
		buf:     p,
	})
	return res.n, res.err
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

func (f *s3File) Reader(ctx context.Context) io.ReadSeeker {
	if f.mode != readonly {
		return file.NewError(fmt.Errorf("reader %v: file is not opened in read mode", f.name))
	}
	return &s3Reader{ctx: ctx, f: f}
}

// s3Writer implements a placeholder io.Writer for S3.
type s3Writer struct {
	ctx context.Context
	f   *s3File
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

func (f *s3File) Writer(ctx context.Context) io.Writer {
	if f.mode != writeonly {
		return file.NewError(fmt.Errorf("writer %v: file is not opened in write mode", f.name))
	}
	return &s3Writer{ctx: ctx, f: f}
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

func (f *s3File) handleRequests() {
	for req := range f.reqCh {
		switch req.reqType {
		case statRequest:
			f.handleStat(req)
		case seekRequest:
			f.handleSeek(req)
		case readRequest:
			f.handleRead(req)
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

func (f *s3File) handleStat(req request) {
	ctx := req.ctx
	clients, err := f.provider.Get(ctx, "GetObject", f.name)
	if err != nil {
		req.ch <- response{err: errors.E(err, fmt.Sprintf("s3file.stat %v", f.name))}
		return
	}
	policy := newRetryPolicy(clients, f.opts)
	info, err := stat(ctx, clients, policy, f.name)
	if err != nil {
		req.ch <- response{err: err}
		return
	}
	f.info = info
	req.ch <- response{err: nil}
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

func (f *s3File) startGetObjectRequest(ctx context.Context, policy *retryPolicy, metric *metricOpProgress) error {
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
		output, err := policy.client().GetObjectWithContext(ctx, input, ids.captureOption())
		if policy.shouldRetry(ctx, err, f.name) {
			metric.Retry()
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
			f.info = &s3Info{
				name:    filepath.Base(f.name),
				size:    *output.ContentLength,
				modTime: *output.LastModified,
				etag:    *output.ETag,
			}
		}
		return nil
	}
}

type maxRetrier interface {
	MaxRetries() int
}

func maxRetries(clients []s3iface.S3API) int {
	for _, client := range clients {
		if s, ok := client.(maxRetrier); ok && s.MaxRetries() > 0 {
			return s.MaxRetries()
		}
	}
	return defaultMaxRetries
}

func (f *s3File) handleRead(req request) {
	buf := req.buf
	clients, err := f.provider.Get(req.ctx, "GetObject", f.name)
	if err != nil {
		req.ch <- response{err: errors.E(err, fmt.Sprintf("s3file.read %v", f.name))}
		return
	}
	metric := metrics.Op("read").Start()
	defer metric.Done()
	maxRetries := maxRetries(clients)
	policy := newRetryPolicy(clients, f.opts)
	retries := 0 // TODO(saito) use retryPolicy instead
	for len(buf) > 0 {
		if f.bodyReader == nil {
			if err = f.startGetObjectRequest(req.ctx, &policy, metric); err != nil {
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
					metric.Retry()
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
	metric.Bytes(totalBytesRead)
	req.ch <- response{n: totalBytesRead, err: err}
}

func (f *s3File) handleWrite(req request) {
	f.uploader.write(req.buf)
	req.ch <- response{n: len(req.buf), err: nil}
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
