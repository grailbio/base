package s3file

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	awsrequest "github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/testutil/assert"
)

func TestS3BucketLister(t *testing.T) {
	lister := s3BucketLister{
		ctx:    context.Background(),
		scheme: "s3",
		clients: []s3iface.S3API{
			listBucketsFakeClient{},
			listBucketsFakeClient{
				buckets: []*s3.Bucket{
					{Name: aws.String("bucketA")},
					{Name: aws.String("bucketC")},
				},
			},
			listBucketsFakeClient{
				buckets: []*s3.Bucket{
					{Name: aws.String("bucketC")},
					{Name: aws.String("bucketB")},
				},
			},
		},
	}

	assert.NoError(t, lister.Err())

	assert.True(t, lister.Scan())
	assert.EQ(t, lister.Path(), "s3://bucketA") // expect alphabetical order
	assert.EQ(t, lister.IsDir(), true)
	_ = lister.Info() // expect nothing, but it must not panic
	assert.NoError(t, lister.Err())

	assert.True(t, lister.Scan())
	assert.EQ(t, lister.Path(), "s3://bucketB")
	assert.EQ(t, lister.IsDir(), true)
	_ = lister.Info()
	assert.NoError(t, lister.Err())

	assert.True(t, lister.Scan())
	assert.EQ(t, lister.Path(), "s3://bucketC")
	assert.EQ(t, lister.IsDir(), true)
	_ = lister.Info()
	assert.NoError(t, lister.Err())

	assert.False(t, lister.Scan())
	assert.NoError(t, lister.Err())
}

type listBucketsFakeClient struct {
	buckets       []*s3.Bucket // stub response
	s3iface.S3API              // all other methods panic with nil dereference
}

func (c listBucketsFakeClient) ListBucketsWithContext(
	aws.Context, *s3.ListBucketsInput, ...awsrequest.Option,
) (*s3.ListBucketsOutput, error) {
	return &s3.ListBucketsOutput{Buckets: c.buckets}, nil
}
