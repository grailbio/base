package s3file

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
)

// Stat implements file.Implementation interface.
func (impl *s3Impl) Stat(ctx context.Context, path string, opts ...file.Opts) (file.Info, error) {
	_, bucket, key, err := ParseURL(path)
	if err != nil {
		return nil, errors.E(errors.Invalid, "could not parse", path, err)
	}
	resp := runRequest(ctx, func() response {
		clients, err := impl.clientsForAction(ctx, "GetObject", bucket, key)
		if err != nil {
			return response{err: err}
		}
		policy := newBackoffPolicy(clients, mergeFileOpts(opts))
		info, err := stat(ctx, clients, policy, path, bucket, key)
		if err != nil {
			return response{err: err}
		}
		return response{info: info}
	})
	return resp.info, resp.err
}

func stat(ctx context.Context, clients []s3iface.S3API, policy retryPolicy, path, bucket, key string) (*s3Info, error) {
	if key == "" {
		return nil, errors.E(errors.Invalid, "cannot stat with empty S3 key", path)
	}
	metric := metrics.Op("stat").Start()
	defer metric.Done()
	for {
		var ids s3RequestIDs
		output, err := policy.client().HeadObjectWithContext(ctx,
			&s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			},
			ids.captureOption(),
		)
		if policy.shouldRetry(ctx, err, path) {
			metric.Retry()
			continue
		}
		if err != nil {
			return nil, annotate(err, ids, &policy, "s3file.stat", path)
		}
		if output.ETag == nil || *output.ETag == "" {
			return nil, errors.E("s3file.stat: empty ETag", path, errors.NotExist, "awsrequestID:", ids.String())
		}
		if output.ContentLength == nil {
			return nil, errors.E("s3file.stat: nil ContentLength", path, errors.NotExist, "awsrequestID:", ids.String())
		}
		if *output.ContentLength == 0 && strings.HasSuffix(path, "/") {
			// Assume this is a directory marker:
			// https://web.archive.org/web/20190424231712/https://docs.aws.amazon.com/AmazonS3/latest/user-guide/using-folders.html
			return nil, errors.E("s3file.stat: directory marker at path", path, errors.NotExist, "awsrequestID:", ids.String())
		}
		if output.LastModified == nil {
			return nil, errors.E("s3file.stat: nil LastModified", path, errors.NotExist, "awsrequestID:", ids.String())
		}
		return &s3Info{
			name:    filepath.Base(path),
			size:    *output.ContentLength,
			modTime: *output.LastModified,
			etag:    *output.ETag,
		}, nil
	}
}
