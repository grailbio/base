package s3util

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/base/traverse"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

const (
	// DefaultS3ObjectCopySizeLimit is the max size of object for a single PUT Object Copy request.
	// As per AWS: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html
	// the max size allowed is 5GB, but we use a smaller size here to speed up large file copies.
	DefaultS3ObjectCopySizeLimit = 256 << 20 // 256MiB

	// defaultS3MultipartCopyPartSize is the max size of each part when doing a multi-part copy.
	// Note: Though we can do parts of size up to defaultS3ObjectCopySizeLimit, for large files
	// using smaller size parts (concurrently) is much faster.
	DefaultS3MultipartCopyPartSize = 128 << 20 // 128MiB

	// s3MultipartCopyConcurrencyLimit is the number of concurrent parts to do during a multi-part copy.
	s3MultipartCopyConcurrencyLimit = 100

	defaultMaxRetries = 3
)

var (
	// DefaultRetryPolicy is the default retry policy
	DefaultRetryPolicy = retry.MaxRetries(retry.Jitter(retry.Backoff(1*time.Second, time.Minute, 2), 0.25), defaultMaxRetries)
)

type Debugger interface {
	Debugf(format string, args ...interface{})
}

type noOpDebugger struct{}

func (d noOpDebugger) Debugf(format string, args ...interface{}) {}

// Copier supports operations to copy S3 objects (within or across buckets)
// by using S3 APIs that support the same (ie, without having to stream the data by reading and writing).
//
// Since AWS doesn't allow copying large files in a single operation,
// this will do a multi-part copy object in those cases.
// However, this behavior can also be controlled by setting appropriate values
// for S3ObjectCopySizeLimit and S3MultipartCopyPartSize.

type Copier struct {
	client  s3iface.S3API
	retrier retry.Policy

	// S3ObjectCopySizeLimit is the max size of object for a single PUT Object Copy request.
	S3ObjectCopySizeLimit int64
	// S3MultipartCopyPartSize is the max size of each part when doing a multi-part copy.
	S3MultipartCopyPartSize int64

	Debugger
}

func NewCopier(client s3iface.S3API) *Copier {
	return NewCopierWithParams(client, DefaultRetryPolicy, DefaultS3ObjectCopySizeLimit, DefaultS3MultipartCopyPartSize, nil)
}

func NewCopierWithParams(client s3iface.S3API, retrier retry.Policy, s3ObjectCopySizeLimit int64, s3MultipartCopyPartSize int64, debugger Debugger) *Copier {
	if debugger == nil {
		debugger = noOpDebugger{}
	}
	return &Copier{
		client:                  client,
		retrier:                 retrier,
		S3ObjectCopySizeLimit:   s3ObjectCopySizeLimit,
		S3MultipartCopyPartSize: s3MultipartCopyPartSize,
		Debugger:                debugger,
	}
}

// Copy copies the S3 object from srcUrl to dstUrl (both expected to be full S3 URLs)
// The size of the source object (srcSize) determines behavior (whether done as single or multi-part copy).
//
// dstMetadata must be set if the caller wishes to set the metadata on the dstUrl object.
// While the AWS API will copy the metadata over if done using CopyObject, but NOT when multi-part copy is done,
// this method requires that dstMetadata be always provided to remove ambiguity.
// So if metadata is desired on dstUrl object, *it must always be provided*.
func (c *Copier) Copy(ctx context.Context, srcUrl, dstUrl string, srcSize int64, dstMetadata map[string]*string) error {
	copySrc := strings.TrimPrefix(srcUrl, "s3://")
	dstBucket, dstKey, err := bucketKey(dstUrl)
	if err != nil {
		return err
	}
	if srcSize <= c.S3ObjectCopySizeLimit {
		// Do single copy
		input := &s3.CopyObjectInput{
			Bucket:     aws.String(dstBucket),
			Key:        aws.String(dstKey),
			CopySource: aws.String(copySrc),
			Metadata:   dstMetadata,
		}
		for retries := 0; ; retries++ {
			_, err = c.client.CopyObjectWithContext(ctx, input)
			err = CtxErr(ctx, err)
			if err == nil {
				break
			}
			severity := Severity(err)
			if severity != errors.Temporary && severity != errors.Retriable {
				break
			}
			c.Debugf("s3copy.Copy: attempt (%d): %s -> %s\n%v\n", retries, srcUrl, dstUrl, err)
			if err = retry.Wait(ctx, c.retrier, retries); err != nil {
				break
			}
		}
		if err == nil {
			c.Debugf("s3copy.Copy: done: %s -> %s", srcUrl, dstUrl)
		}
		return err
	}
	// Do a multi-part copy
	numParts := (srcSize + c.S3MultipartCopyPartSize - 1) / c.S3MultipartCopyPartSize
	input := &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(dstBucket),
		Key:      aws.String(dstKey),
		Metadata: dstMetadata,
	}
	createOut, err := c.client.CreateMultipartUploadWithContext(ctx, input)
	if err != nil {
		return errors.E(fmt.Sprintf("CreateMultipartUpload: %s -> %s", srcUrl, dstUrl), err)
	}
	completedParts := make([]*s3.CompletedPart, numParts)
	err = traverse.Limit(s3MultipartCopyConcurrencyLimit).Each(int(numParts), func(ti int) error {
		i := int64(ti)
		firstByte := i * c.S3MultipartCopyPartSize
		lastByte := firstByte + c.S3MultipartCopyPartSize - 1
		if lastByte >= srcSize {
			lastByte = srcSize - 1
		}
		var partErr error
		var uploadOut *s3.UploadPartCopyOutput
		for retries := 0; ; retries++ {
			uploadOut, partErr = c.client.UploadPartCopyWithContext(ctx, &s3.UploadPartCopyInput{
				Bucket:          aws.String(dstBucket),
				Key:             aws.String(dstKey),
				CopySource:      aws.String(copySrc),
				UploadId:        createOut.UploadId,
				PartNumber:      aws.Int64(i + 1),
				CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", firstByte, lastByte)),
			})
			partErr = CtxErr(ctx, partErr)
			if partErr == nil {
				break
			}
			severity := Severity(partErr)
			if severity != errors.Temporary && severity != errors.Retriable {
				break
			}
			c.Debugf("s3copy.Copy: attempt (%d) (part %d/%d): %s -> %s\n%v\n", retries, i, numParts, srcUrl, dstUrl, partErr)
			if partErr = retry.Wait(ctx, c.retrier, retries); partErr != nil {
				break
			}
		}
		if partErr == nil {
			completedParts[i] = &s3.CompletedPart{ETag: uploadOut.CopyPartResult.ETag, PartNumber: aws.Int64(i + 1)}
			c.Debugf("s3copy.Copy: done (part %d/%d): %s -> %s", i, numParts, srcUrl, dstUrl)
			return nil
		}
		return errors.E(fmt.Sprintf("upload part copy (part %d/%d) %s -> %s", i, numParts, srcUrl, dstUrl), partErr)
	})
	if err == nil {
		// Complete the multi-part copy
		for retries := 0; ; retries++ {
			_, err = c.client.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
				Bucket:          aws.String(dstBucket),
				Key:             aws.String(dstKey),
				UploadId:        createOut.UploadId,
				MultipartUpload: &s3.CompletedMultipartUpload{Parts: completedParts},
			})
			if err == nil || Severity(err) != errors.Temporary {
				break
			}
			c.Debugf("s3copy.Copy complete upload: attempt (%d): %s -> %s\n%v\n", retries, srcUrl, dstUrl, err)
			if err = retry.Wait(ctx, c.retrier, retries); err != nil {
				break
			}
		}
		if err == nil {
			c.Debugf("s3copy.Copy: done (all %d parts): %s -> %s", numParts, srcUrl, dstUrl)
			return nil
		}
		err = errors.E(fmt.Sprintf("complete multipart upload %s -> %s", srcUrl, dstUrl), Severity(err), err)
	}
	// Abort the multi-part copy
	if _, er := c.client.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(dstBucket),
		Key:      aws.String(dstKey),
		UploadId: createOut.UploadId,
	}); er != nil {
		err = fmt.Errorf("abort multipart copy %v (aborting due to original error: %v)", er, err)
	}
	return err
}

// bucketKey returns the bucket and key for the given S3 object url and error (if any).
func bucketKey(rawurl string) (string, string, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return "", "", errors.E(errors.Invalid, errors.Fatal, fmt.Sprintf("cannot determine bucket and key from rawurl %s", rawurl), err)
	}
	bucket := u.Host
	return bucket, strings.TrimPrefix(rawurl, "s3://"+bucket+"/"), nil
}
