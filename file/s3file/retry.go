package s3file

import (
	"context"
	"strings"
	"time"

	awsrequest "github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/retry"
)

var (
	// BackoffPolicy defines backoff timing parameters. It is exposed publicly only
	// for unittests.
	// TODO(josh): Rename to `RetryPolicy`.
	// TODO(josh): Create `retry.ThrottlePolicy` and `retry.AIMDPolicy` and use here.
	BackoffPolicy = retry.Jitter(retry.Backoff(500*time.Millisecond, time.Minute, 1.2), 0.2)

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

// TODO: Rename to `retrier`.
type retryPolicy struct {
	clients       []s3iface.S3API
	policy        retry.Policy
	opts          file.Opts // passed to Open() or Stat request.
	startTime     time.Time // the time requested started.
	retryDeadline time.Time // when to give up retrying.
	retries       int
	waitErr       error // error happened during wait, typically deadline or cancellation.
}

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
		aerr.Code() == "InternalError" ||
		aerr.Code() == "InternalServerError") {
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
