// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package s3file

import (
	"context"
	"flag"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	awsrequest "github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var awsFlag = flag.Bool("aws", false, "If true, run tests that access AWS.")

func TestBucketRegion(t *testing.T) {
	if !*awsFlag {
		t.Skipf("skipping %s, pass -aws to run", t.Name())
		return
	}

	ctx := context.Background()
	region := findBucketRegion(t, ctx, "grail-ccga2-evaluation-runs")
	require.Equal(t, region, "us-west-2")

	region = findBucketRegion(t, ctx, "grail-test-us-east-1")
	require.Equal(t, region, "us-east-1")

	region = findBucketRegion(t, ctx, "grail-test-us-east-2")
	require.Equal(t, region, "us-east-2")
}

func findBucketRegion(t *testing.T, ctx context.Context, bucket string) string {
	region, err := FindBucketRegion(ctx, bucket)
	require.NoError(t, err)
	return region
}

func TestBucketRegionCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = <-ctx.Done()
	cache := bucketRegionCache{
		getBucketRegionWithClient: func(aws.Context, s3iface.S3API, string, ...awsrequest.Option) (string, error) {
			return "", errors.E(errors.Temporary, "test transient error")
		},
	}
	_, err := cache.locate(ctx, "grail-ccga2-evaluation-runs")
	assert.Contains(t, err.Error(), context.Canceled.Error())
}
