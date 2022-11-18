package s3file

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/ioctx"
)

// s3File implements file.File interface.
//
// Operations on a file are internally implemented by a goroutine running handleRequests,
// which reads requests from s3file.reqCh and sends responses to request.ch.
//
// s3File's API methods (Read, Seek, etc.) are implemented by:
// - Create a chan response.
// - Construct a request{} object describing the operation and send it to reqCh.
// - Wait for a message from either the response channel or context.Done(),
// whichever comes first.
type s3File struct {
	name             string // "s3://bucket/key/.."
	clientsForAction clientsForActionFunc
	mode             accessMode
	opts             file.Opts

	bucket string // bucket part of "name".
	key    string // key part "name".

	// info is file metadata. Set at construction if mode == readonly, otherwise nil.
	info *s3Info

	bodyReader chunkReaderCache

	// reqCh transports user operations (like Read) to the worker goroutine (handleRequests).
	// This allows respecting context cancellation (regardless of what underlying AWS SDK operations
	// do). It also guards subsequent fields; they are only accessed by the handleRequests
	// goroutine.
	reqCh chan request

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
	return err
}

func (f *s3File) Discard(ctx context.Context) {
	if f.mode != writeonly {
		return
	}
	_ = f.runRequest(ctx, request{reqType: abortRequest})
	close(f.reqCh)
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
		return response{err: errors.E(errors.Canceled)}
	}
}

func (f *s3File) handleStat(req request) {
	ctx := req.ctx
	clients, err := f.clientsForAction(ctx, "GetObject", f.bucket, f.key)
	if err != nil {
		req.ch <- response{err: errors.E(err, fmt.Sprintf("s3file.stat %v", f.name))}
		return
	}
	policy := newBackoffPolicy(clients, f.opts)
	info, err := stat(ctx, clients, policy, f.name, f.bucket, f.key)
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
	req.ch <- response{off: f.position}
}

var _ ioctx.ReaderAt = (*s3File)(nil)

func (f *s3File) ReaderAt() ioctx.ReaderAt { return f }

// TODO: Stop implementing ReaderAt in *localFile, instead return a different object from ReaderAt.
func (f *s3File) ReadAt(ctx context.Context, buf []byte, off int64) (int, error) {
	if f.mode != readonly {
		return 0, errors.E(errors.NotAllowed, "not opened for read")
	}
	if f.info == nil {
		panic("stat not filled")
	}

	reader, cleanUp, err := f.bodyReader.getOrCreate(ctx, func() (*chunkReaderAt, error) {
		clients, err := f.clientsForAction(ctx, "GetObject", f.bucket, f.key)
		if err != nil {
			return nil, errors.E(err, "getting clients")
		}
		return &chunkReaderAt{
			name:   f.name,
			bucket: f.bucket,
			key:    f.key,
			newRetryPolicy: func() retryPolicy {
				return newBackoffPolicy(append([]s3iface.S3API{}, clients...), f.opts)
			},
		}, nil
	})
	if err != nil {
		return 0, err
	}
	defer cleanUp()

	var n int
	// Note: We allow seeking past EOF, consistent with io.Seeker.Seek's documentation. We simply
	// return EOF in this situation.
	if bytesUntilEOF := f.info.size - off; bytesUntilEOF <= 0 {
		err = io.EOF
	} else {
		// Because we know the size of the object, pass a smaller buffer to the
		// chunk reader to save it the effort of trying to fill it (with
		// parallel reads). This is an optimization that does not affect
		// correctness.
		// TODO: Consider how to move this optimization into the chunk reader
		// itself, possibly by optionally passing in the size/metadata.
		if len(buf) > int(bytesUntilEOF) {
			buf = buf[:bytesUntilEOF]
		}
		var info s3Info
		n, info, err = reader.ReadAt(ctx, buf, off)
		if err != nil && err != io.EOF {
			err = errors.E(err, fmt.Sprintf("s3file.read %v", f.name))
		} else if info == (s3Info{}) {
			// Maybe EOF or len(req.buf) == 0.
		} else if f.info.etag != info.etag {
			// Note: If err was io.EOF, we intentionally drop that in favor of flagging ETag mismatch.
			err = eTagChangedError(f.name, f.info.etag, info.etag)
		}
	}
	return n, err
}

func (f *s3File) handleRead(req request) {
	n, err := f.ReadAt(req.ctx, req.buf, f.position)
	f.position += int64(n)
	req.ch <- response{n: n, err: err}
}

func (f *s3File) handleWrite(req request) {
	f.uploader.write(req.buf)
	req.ch <- response{n: len(req.buf), err: nil}
}

func (f *s3File) handleClose(req request) {
	var err error
	if f.uploader != nil {
		err = f.uploader.finish()
	}
	f.bodyReader.close()
	if err != nil {
		err = errors.E(err, "s3file.close", f.name)
	}
	f.clientsForAction = nil
	req.ch <- response{err: err}
}

func (f *s3File) handleAbort(req request) {
	err := f.uploader.abort()
	if err != nil {
		err = errors.E(err, "s3file.abort", f.name)
	}
	f.clientsForAction = nil
	req.ch <- response{err: err}
}
