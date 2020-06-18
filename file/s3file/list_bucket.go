package s3file

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/base/file"
)

type s3BucketLister struct {
	ctx     context.Context
	policy  retryPolicy
	scheme  string
	err     error
	listed  bool
	bucket  string
	buckets []string
}

func (l *s3BucketLister) Scan() bool {
	if !l.listed {
		for {
			var ids s3RequestIDs
			res, err := l.policy.client().ListBucketsWithContext(l.ctx, &s3.ListBucketsInput{},
				ids.captureOption())
			if l.policy.shouldRetry(l.ctx, err, "listbuckets") {
				continue
			}
			if err != nil {
				l.err = annotate(err, ids, &l.policy, "s3file.listbuckets")
				return false
			}
			for _, bucket := range res.Buckets {
				l.buckets = append(l.buckets, *bucket.Name)
			}
			break
		}
		l.listed = true
	}
	if len(l.buckets) == 0 {
		return false
	}
	l.bucket, l.buckets = l.buckets[0], l.buckets[1:]
	return true
}

func (l *s3BucketLister) Path() string {
	return fmt.Sprintf("%s://%s", l.scheme, l.bucket)
}

func (l *s3BucketLister) Info() file.Info { return nil }

func (l *s3BucketLister) IsDir() bool {
	return true
}

// Err returns an error, if any.
func (l *s3BucketLister) Err() error {
	return l.err
}
