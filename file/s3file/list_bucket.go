package s3file

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
)

type s3BucketLister struct {
	ctx     context.Context
	clients []s3iface.S3API
	scheme  string

	err     error
	listed  bool
	bucket  string
	buckets []string
}

func (l *s3BucketLister) Scan() bool {
	if !l.listed {
		l.buckets, l.err = combineClientBuckets(l.ctx, l.clients)
		l.listed = true
	}
	if l.err != nil || len(l.buckets) == 0 {
		return false
	}
	l.bucket, l.buckets = l.buckets[0], l.buckets[1:]
	return true
}

// combineClientBuckets returns the union of buckets from each client, since each may have
// different permissions.
func combineClientBuckets(ctx context.Context, clients []s3iface.S3API) ([]string, error) {
	var (
		uniqueBucketsMu sync.Mutex
		uniqueBuckets   = map[string]struct{}{}
	)
	err := traverse.Parallel.Each(len(clients), func(clientIdx int) error {
		buckets, err := listClientBuckets(ctx, clients[clientIdx])
		if err != nil {
			if errors.Is(errors.NotAllowed, err) {
				log.Debug.Printf("s3file.listbuckets: ignoring: %v", err)
				return nil
			}
			return err
		}
		uniqueBucketsMu.Lock()
		defer uniqueBucketsMu.Unlock()
		for _, bucket := range buckets {
			uniqueBuckets[bucket] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	buckets := make([]string, 0, len(uniqueBuckets))
	for bucket := range uniqueBuckets {
		buckets = append(buckets, bucket)
	}
	sort.Strings(buckets)
	return buckets, nil
}

func listClientBuckets(ctx context.Context, client s3iface.S3API) ([]string, error) {
	policy := newRetryPolicy([]s3iface.S3API{client}, file.Opts{})
	for {
		var ids s3RequestIDs
		res, err := policy.client().ListBucketsWithContext(ctx, &s3.ListBucketsInput{}, ids.captureOption())
		if policy.shouldRetry(ctx, err, "listbuckets") {
			continue
		}
		if err != nil {
			return nil, annotate(err, ids, &policy, "s3file.listbuckets")
		}
		buckets := make([]string, len(res.Buckets))
		for i, bucket := range res.Buckets {
			buckets[i] = *bucket.Name
		}
		return buckets, nil
	}
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
