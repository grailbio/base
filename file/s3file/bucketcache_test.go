// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build arc-ignore phabricator-ignore

package s3file_test

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/base/file/s3file"
	"github.com/stretchr/testify/require"
)

var (
	manualFlag = flag.Bool("run-manual-test", false, "If true, run tests that access AWS.")
)

func maybeSkipManualTest(t *testing.T) {
	if *manualFlag {
		return
	}
	if os.Getenv("TEST_TMPDIR") == "" {
		return
	}
	t.Skip("not enabled")
}

func getBucketRegion(t *testing.T, ctx context.Context, bucket string) string {
	sess, err := session.NewSession(&aws.Config{
		MaxRetries: aws.Int(10),
		Region:     aws.String("us-east-1"),
	})
	require.NoError(t, err)
	client := s3.New(sess)
	region, err := s3file.GetBucketRegion(ctx, client, bucket)
	require.NoError(t, err)
	return region
}

func TestBucketRegion(t *testing.T) {
	maybeSkipManualTest(t)

	ctx := context.Background()
	region := getBucketRegion(t, ctx, "grail-ysaito")
	require.Equal(t, region, "us-west-2")

	region = getBucketRegion(t, ctx, "grail-test-us-east-1")
	require.Equal(t, region, "us-east-1")

	region = getBucketRegion(t, ctx, "grail-test-us-east-2")
	require.Equal(t, region, "us-east-2")
}
