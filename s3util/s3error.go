package s3util

import (
	"context"

	"github.com/grailbio/base/errors"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

// CtxErr will return the context's error (if any) or the other error.
// This is particularly useful to interpret AWS S3 API call errors
// because AWS sometimes wraps context errors (context.Canceled or context.DeadlineExceeded).
func CtxErr(ctx context.Context, other error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return other
}

// KindAndSeverity interprets a given error and returns errors.Severity.
// This is particularly useful to interpret AWS S3 API call errors.
func Severity(err error) errors.Severity {
	if aerr, ok := err.(awserr.Error); ok {
		_, severity := KindAndSeverity(aerr)
		return severity
	}
	if re := errors.Recover(err); re != nil {
		return re.Severity
	}
	return errors.Unknown
}

// KindAndSeverity interprets a given error and returns errors.Kind and errors.Severity.
// This is particularly useful to interpret AWS S3 API call errors.
func KindAndSeverity(err error) (errors.Kind, errors.Severity) {
	for {
		if request.IsErrorThrottle(err) {
			return errors.ResourcesExhausted, errors.Temporary
		}
		if request.IsErrorRetryable(err) {
			return errors.Other, errors.Temporary
		}
		aerr, ok := err.(awserr.Error)
		if !ok {
			break
		}
		if aerr.Code() == request.CanceledErrorCode {
			return errors.Canceled, errors.Fatal
		}
		// The underlying error was an S3 error. Try to classify it.
		// Best guess based on Amazon's descriptions:
		switch aerr.Code() {
		// Code NotFound is not documented, but it's what the API actually returns.
		case s3.ErrCodeNoSuchBucket, "NoSuchVersion", "NotFound":
			return errors.NotExist, errors.Fatal
		case s3.ErrCodeNoSuchKey:
			// Treat as temporary because sometimes they are, due to S3's eventual consistency model
			// https://aws.amazon.com/premiumsupport/knowledge-center/404-error-nosuchkey-s3/
			return errors.NotExist, errors.Temporary
		case "AccessDenied":
			return errors.NotAllowed, errors.Fatal
		case "InvalidRequest", "InvalidArgument", "EntityTooSmall", "EntityTooLarge", "KeyTooLong", "MethodNotAllowed":
			return errors.Invalid, errors.Fatal
		case "ExpiredToken", "AccountProblem", "ServiceUnavailable", "TokenRefreshRequired", "OperationAborted":
			return errors.Unavailable, errors.Fatal
		case "PreconditionFailed":
			return errors.Precondition, errors.Fatal
		case "SlowDown":
			return errors.ResourcesExhausted, errors.Temporary
		case "BadRequest":
			return errors.Other, errors.Temporary
		case "InternalError":
			// AWS recommends retrying InternalErrors:
			// https://aws.amazon.com/premiumsupport/knowledge-center/s3-resolve-200-internalerror/
			// https://aws.amazon.com/premiumsupport/knowledge-center/http-5xx-errors-s3/
			return errors.Other, errors.Retriable
		case "XAmzContentSHA256Mismatch":
			// Example:
			//
			// XAmzContentSHA256Mismatch: The provided 'x-amz-content-sha256' header
			// does not match what was computed.
			//
			// Happens sporadically for no discernible reason.  Just retry.
			return errors.Other, errors.Temporary
		// "RequestError"s are not considered retryable by `request.IsErrorRetryable(err)`
		// if the underlying cause is due to a "read: connection reset".  For explanation, see:
		// https://github.com/aws/aws-sdk-go/issues/2525#issuecomment-519263830
		// So we catch all "RequestError"s here as temporary.
		case request.ErrCodeRequestError:
			return errors.Other, errors.Temporary
		// "SerializationError"s are not considered retryable by `request.IsErrorRetryable(err)`
		// if the underlying cause is due to a "read: connection reset".  For explanation, see:
		// https://github.com/aws/aws-sdk-go/issues/2525#issuecomment-519263830
		// So we catch all "SerializationError"s here as temporary.
		case request.ErrCodeSerialization:
			return errors.Other, errors.Temporary
		}
		if aerr.OrigErr() == nil {
			break
		}
		err = aerr.OrigErr()
	}
	return errors.Other, errors.Unknown
}
