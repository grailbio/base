package s3file

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awserr"
	awsrequest "github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/base/errors"
)

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

func (ids *s3RequestIDs) captureOption() awsrequest.Option {
	h0 := withGetResponseHeaderWithNilCheck("x-amz-request-id", &ids.amzRequestID)
	h1 := withGetResponseHeaderWithNilCheck("x-amz-id-2", &ids.amzID2)
	return func(r *awsrequest.Request) {
		h0(r)
		h1(r)
	}
}
