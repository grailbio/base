package s3file

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file/s3file/internal/autolog"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
)

type (
	// chunkReaderAt is similar to ioctx.ReaderAt except it is not concurrency-safe.
	// It's currently used to implement S3-recommended read parallelism for large reads, though
	// clients of s3file still only see the non-parallel io.Reader API.
	// TODO: Expose concurrency-safe ReaderAt API to clients.
	chunkReaderAt struct {
		// name is redundant with (bucket, key).
		name, bucket, key string
		// newRetryPolicy creates retry policies. It must be concurrency- and goroutine-safe.
		newRetryPolicy func() retryPolicy

		// previousR is a body reader open from a previous ReadAt. It's an optimization for
		// clients that do many small reads. It may be nil (before first read, after errors, etc.).
		previousR *posReader
		// chunks is used locally within ReadAt. It's stored here only to reduce allocations.
		chunks []readChunk
	}
	readChunk struct {
		// s3Offset is the position of this *chunk* in the coordinates of the S3 object.
		// That is, dst[0] will eventually contain s3Object[s3Offset].
		s3Offset int64
		// dst contains the chunk's data after read. After read, dstN < len(dst) iff there was an
		// error or EOF.
		dst []byte
		// dstN tracks how much of dst is already filled.
		dstN int
		// r is the current reader for this chunk. It may be nil or at the wrong position for
		// this chunk's state; then we'd need a new reader.
		r *posReader
	}

	// posReader wraps the S3 SDK's reader with retries and remembers its offset in the S3 object.
	posReader struct {
		rc     io.ReadCloser
		offset int64
		// ids is set when posReader is opened.
		ids s3RequestIDs
		// info is set when posReader is opened, unless there's an error or EOF.
		info s3Info
	}
)

// ReadChunkBytes is the size for individual S3 API read operations, guided by S3 docs:
//   As a general rule, when you download large objects within a Region from Amazon S3 to
//   Amazon EC2, we suggest making concurrent requests for byte ranges of an object at the
//   granularity of 8–16 MB.
//   https://web.archive.org/web/20220325121400/https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html
// It's exposed for tests only and client should not use it.
var ReadChunkBytes int64 = 16 * 1024 * 1024

// ReadAt is not concurrency-safe.
// s3Info may be empty if no object metadata is fetched (zero-sized request, error).
func (r *chunkReaderAt) ReadAt(ctx context.Context, dst []byte, offset int64) (int, s3Info, error) {
	if len(dst) == 0 {
		return 0, s3Info{}, nil
	}
	r.chunks = r.chunks[:0]
	for buf, bufOff := dst, offset; len(buf) > 0; {
		size := int64(len(buf))
		if size > ReadChunkBytes {
			size = ReadChunkBytes
		}
		r.chunks = append(r.chunks, readChunk{
			s3Offset: bufOff,
			dst:      buf[:size:size],
		})
		bufOff += size
		buf = buf[size:]
	}

	// The first chunk gets to try to use a previously-opened reader (best-effort).
	// Note: If len(r.chunks) == 1 we're both reusing a saved reader and saving it again.
	r.chunks[0].r, r.previousR = r.previousR, nil
	defer func() {
		r.previousR = r.chunks[len(r.chunks)-1].r
	}()

	var (
		infoMu sync.Mutex
		info   s3Info
	)
	// TODO: traverse (or other common lib) support for exiting on first error to reduce latency.
	err := traverse.Each(len(r.chunks), func(chunkIdx int) (err error) {
		chunk := &r.chunks[chunkIdx]
		policy := r.newRetryPolicy()

		defer func() {
			if err != nil {
				err = annotate(err, chunk.r.maybeIDs(), &policy)
			}
		}()
		// Leave the last chunk's reader open for future reuse.
		if chunkIdx < len(r.chunks)-1 {
			defer func() { chunk.r.Close(); chunk.r = nil }()
		}

		metric := metrics.Op("read").Start()
		defer metric.Done()

		for attempt := 0; ; attempt++ {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				err = nil
				break
			}
			if err != nil && !policy.shouldRetry(ctx, err, r.name) {
				break
			}
			err = nil
			if attempt > 0 {
				metric.Retry()
			}

			rangeStart := chunk.s3Offset + int64(chunk.dstN)
			switch {
			case chunk.r != nil && chunk.r.offset == rangeStart:
				// We're ready to read.
			case chunk.r != nil:
				chunk.r.Close()
				fallthrough
			default:
				chunk.r, err = newPosReader(ctx, policy.client(), r.name, r.bucket, r.key, rangeStart)
				if err != nil {
					continue
				}
			}

			if chunk.r.info != (s3Info{}) {
				infoMu.Lock()
				if info == (s3Info{}) {
					info = chunk.r.info
				} else if info.etag != chunk.r.info.etag {
					err = eTagChangedError(r.name, info.etag, chunk.r.info.etag)
				}
				infoMu.Unlock()
			}
			if err != nil {
				continue
			}

			var n int
			n, err = io.ReadFull(chunk.r, chunk.dst[chunk.dstN:])
			chunk.dstN += n
			if err == nil {
				break
			}
			// Discard our reader after an error. This error is often due to throttling
			// (especially connection reset), so we want to retry with a new HTTP request which
			// may go to a new host.
			chunk.r.Close()
			chunk.r = nil
		}
		metric.Bytes(chunk.dstN)
		return err
	})

	var nBytes int
	for _, chunk := range r.chunks {
		nBytes += chunk.dstN
		if chunk.dstN < len(chunk.dst) {
			if err == nil {
				err = io.EOF
			}
			break
		}
	}
	return nBytes, info, err
}

func eTagChangedError(name, oldETag, newETag string) error {
	return errors.E(errors.Precondition, fmt.Sprintf(
		"read %v: ETag changed from %v to %v", name, oldETag, newETag))
}

func (r *chunkReaderAt) Close() { r.previousR.Close() }

var (
	nOpenPos     int32
	nOpenPosOnce sync.Once
)

func newPosReader(
	ctx context.Context,
	client s3iface.S3API,
	name, bucket, key string,
	offset int64,
) (*posReader, error) {
	nOpenPosOnce.Do(func() {
		autolog.Register(func() {
			log.Printf("s3file open posReader: %d", atomic.LoadInt32(&nOpenPos))
		})
	})
	r := posReader{offset: offset}
	output, err := client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-", r.offset)),
	}, r.ids.captureOption())
	if err != nil {
		if output.Body != nil {
			if errClose := output.Body.Close(); errClose != nil {
				log.Printf("s3file.newPosReader: ignoring body close error: %v", err)
			}
		}
		if awsErr, ok := getAWSError(err); ok && awsErr.Code() == "InvalidRange" {
			// Since we're reading many chunks in parallel, some can be past the end of
			// the object, resulting in range errors. Treat these as EOF.
			err = io.EOF
		}
		return nil, err
	}
	_ = atomic.AddInt32(&nOpenPos, 1)
	r.rc, r.offset = output.Body, offset
	if output.ETag != nil && *output.ETag != "" {
		r.info = s3Info{
			name:    filepath.Base(name),
			size:    *output.ContentLength,
			modTime: *output.LastModified,
			etag:    *output.ETag,
		}
	}
	return &r, nil
}

// Read usually delegates to the underlying reader, except: (&posReader{}).Read is valid and
// always at EOF; nil.Read panics.
func (p *posReader) Read(dst []byte) (int, error) {
	if p.rc == nil {
		return 0, io.EOF
	}
	n, err := p.rc.Read(dst)
	p.offset += int64(n)
	return n, err
}

// Close usually delegates to the underlying reader, except: (&posReader{}).Close
// and nil.Close do nothing.
func (p *posReader) Close() {
	if p == nil || p.rc == nil {
		return
	}
	_ = atomic.AddInt32(&nOpenPos, -1)
	if err := p.rc.Close(); err != nil {
		// Note: Since the caller is already done reading from p.rc, we don't expect this error to
		// indicate a problem with the correctness of past Reads, instead signaling some resource
		// leakage (network connection, buffers, etc.). We can't retry the resource release:
		//   * io.Closer does not define behavior for multiple Close calls and
		//     s3.GetObjectOutput.Body doesn't say anything implementation-specific.
		//   * Body may be a net/http.Response.Body [1] but the standard library doesn't say
		//     anything about multiple Close either (and even if it did, we shouldn't rely on the
		//     AWS SDK's implementation details in all cases or in the future).
		// Without a retry opportunity, it seems like callers could either ignore the potential
		// leak, or exit the OS process. We assume, for now, that callers won't want to do the
		// latter, so we hide the error. (This could eventually lead to OS process exit due to
		// resource exhaustion, so arguably this hiding doesn't add much harm, though of course it
		// may be confusing.) We could consider changing this in the future, especially if we notice
		// such resource leaks in real programs.
		//
		// [1] https://github.com/aws/aws-sdk-go/blob/e842504a6323096540dc3defdc7cb357d8749893/private/protocol/rest/unmarshal.go#L89-L90
		log.Printf("s3file.posReader.Close: ignoring body close error: %v", err)
	}
}

// maybeIDs returns ids if available, otherwise zero. p == nil is allowed.
func (p *posReader) maybeIDs() s3RequestIDs {
	if p == nil {
		return s3RequestIDs{}
	}
	return p.ids
}
