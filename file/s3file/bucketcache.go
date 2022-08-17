// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package s3file

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	awsrequest "github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/sync/loadingcache"
)

// bucketRegionCacheDuration is chosen fairly arbitrarily. We expect region changes to be
// extremely rare (deleting a bucket, then recreating elsewhere) so a long time seems fine.
const bucketRegionCacheDuration = time.Hour

// FindBucketRegion locates the AWS region in which bucket is located.
// The lookup is cached internally.
//
// It assumes the region is in the "aws" partition, not other partitions like "aws-us-gov".
// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html
func FindBucketRegion(ctx context.Context, bucket string) (string, error) {
	return globalBucketRegionCache.locate(ctx, bucket)
}

type bucketRegionCache struct {
	cache loadingcache.Map
	// getBucketRegionWithClient indirectly references s3manager.GetBucketRegionWithClient to
	// allow unit testing.
	getBucketRegionWithClient func(ctx aws.Context, svc s3iface.S3API, bucket string, opts ...awsrequest.Option) (string, error)
}

var (
	globalBucketRegionCache = bucketRegionCache{
		getBucketRegionWithClient: s3manager.GetBucketRegionWithClient,
	}
	bucketRegionClient = s3.New(
		session.Must(session.NewSessionWithOptions(session.Options{
			Config: aws.Config{
				// This client is only used for looking up bucket locations, which doesn't
				// require any credentials.
				Credentials: credentials.AnonymousCredentials,
				// Note: This region is just used to infer the relevant AWS partition (group of
				// regions). This would fail for, say, "aws-us-gov", but we only use "aws".
				// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html
				Region: aws.String("us-west-2"),
			},
			SharedConfigState: session.SharedConfigDisable,
		})),
	)
)

func (c *bucketRegionCache) locate(ctx context.Context, bucket string) (string, error) {
	var region string
	err := c.cache.
		GetOrCreate(bucket).
		GetOrLoad(ctx, &region, func(ctx context.Context, opts *loadingcache.LoadOpts) (err error) {
			opts.CacheFor(bucketRegionCacheDuration)
			policy := newBackoffPolicy([]s3iface.S3API{bucketRegionClient}, file.Opts{})
			for {
				var ids s3RequestIDs
				region, err = c.getBucketRegionWithClient(ctx,
					bucketRegionClient, bucket, ids.captureOption())
				if err == nil {
					return nil
				}
				if !policy.shouldRetry(ctx, err, fmt.Sprintf("locate region: %s", bucket)) {
					return annotate(err, ids, &policy)
				}
			}
		})
	if err != nil {
		return "", err
	}
	return region, nil
}
