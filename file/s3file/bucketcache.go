// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package s3file

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
) // bucketCache is a singleton cache manager.
type bucketCache struct {
	mu    sync.Mutex
	cache map[string]string // maps S3 bucket to region (e.g., "us-east-2").
}

var bCache = bucketCache{
	cache: make(map[string]string),
}

// Find finds the region of the bucket using the given s3client. The client need
// not be in the same region as bucket.
func (c *bucketCache) find(ctx context.Context, client s3iface.S3API, bucket string) (string, error) {
	c.mu.Lock()
	val, ok := c.cache[bucket]
	c.mu.Unlock()
	if ok { // Common case
		return val, nil
	}
	region, err := s3manager.GetBucketRegionWithClient(ctx, client, bucket)
	if err != nil {
		return "", err
	}
	// nil location means us-east-1.
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETlocation.html
	if region == "" {
		region = "us-east-1"
	}
	c.mu.Lock()
	c.cache[bucket] = region
	c.mu.Unlock()
	return region, nil
}

// Set overrides the region for the provided bucket.
func (c *bucketCache) set(bucket, region string) {
	c.mu.Lock()
	c.cache[bucket] = region
	c.mu.Unlock()
}

// Invalidate removes the cached bucket-to-region mapping.
func (c *bucketCache) invalidate(bucket string) {
	c.mu.Lock()
	delete(c.cache, bucket)
	c.mu.Unlock()
}

// GetBucketRegion finds the AWS region for the S3 bucket and inserts it in the
// cache. "client" is used to issue the GetBucketRegion S3 call. It doesn't need
// to be in the region for the "bucket".
func GetBucketRegion(ctx context.Context, client s3iface.S3API, bucket string) (string, error) {
	return bCache.find(ctx, client, bucket)
}

// InvalidateBucketRegion  removes the cache entry for bucket, if it exists.
func InvalidateBucketRegion(bucket string) {
	bCache.invalidate(bucket)
}

// SetBucketRegion sets a bucket's region, overriding region discovery and
// defaults.
func SetBucketRegion(bucket, region string) {
	bCache.set(bucket, region)
}
