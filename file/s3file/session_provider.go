// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package s3file

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
)

const defaultRegion = "us-west-2"

type (
	// SessionProvider provides Sessions for making AWS API calls. Get() is called whenever s3file
	// needs to access a file. The provider should cache and reuse the sessions, if needed.
	// The implementation must be thread safe.
	SessionProvider interface {
		// Get returns AWS sessions that can be used to perform in.S3IAMAction on
		// s3://{in.bucket}/{in.key}.
		//
		// s3file will maintain internal references to every *session.Session that are never
		// released, so implementations must not return too many unique ones.
		// Get() is called for every S3 operation so it should be very fast. Caching is strongly
		// encouraged for both performance and avoiding leaking objects.
		//
		// Get() must return >= 1 session, or error. If > 1, the S3 operation will be tried
		// on each session in unspecified order until it succeeds.
		//
		// Note: Some implementations will not need SessionProviderInput and can just ignore it.
		//
		// TODO: Consider passing chan<- *session.Session (implementer sends and then closes)
		// so s3file can try credentials as soon as they're available.
		Get(_ context.Context, in SessionProviderInput) ([]*session.Session, error)
	}
	SessionProviderInput struct {
		// S3IAMAction is an action name from this list:
		// https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html
		//
		// Note: There is no `s3:` prefix.
		//
		// Note: This is different from the notion of "action" in the S3 API documentation:
		// https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html
		// Some names, like GetObject, appear in both; others, like HeadObject, do not.
		S3IAMAction string
		// Bucket and Key describe the API operation to be performed, if applicable.
		Bucket, Key string
	}

	constSessionProvider struct {
		session *session.Session
		err     error
	}
)

// NewDefaultProvider returns a SessionProvider that calls session.NewSession(configs...) once.
func NewDefaultProvider(configs ...*aws.Config) SessionProvider {
	session, err := session.NewSession(configs...)
	return constSessionProvider{session, err}
}

func (p constSessionProvider) Get(context.Context, SessionProviderInput) ([]*session.Session, error) {
	if p.err != nil {
		return nil, p.err
	}
	return []*session.Session{p.session}, nil
}

type (
	clientsForActionFunc func(ctx context.Context, s3IAMAction, bucket, key string) ([]s3iface.S3API, error)
	// clientCache caches clients for all regions, based on the user's SessionProvider.
	clientCache struct {
		provider SessionProvider
		// clients maps clientCacheKey -> *s3.S3.
		// TODO: Implement some kind of garbage collection and relax the documented constraint
		// that sessions are never released.
		clients sync.Map
	}
	clientCacheKey struct {
		region string
		// userSession is the session that the user's SessionProvider returned.
		// It may be configured for a different region, so we don't use it directly.
		userSession *session.Session
	}
)

func newClientCache(provider SessionProvider) *clientCache {
	return &clientCache{provider: provider}
}

func (c *clientCache) forAction(ctx context.Context, s3IAMAction, bucket, key string) ([]s3iface.S3API, error) {
	// TODO: Consider using some better default, like current region if we're in EC2.
	region := defaultRegion
	if bucket != "" { // bucket is empty when listing buckets, for example.
		var err error
		region, err = FindBucketRegion(ctx, bucket)
		if err != nil {
			return nil, errors.E(err, fmt.Sprintf("locating region for bucket %s", bucket))
		}
	}
	sessions, err := c.provider.Get(ctx, SessionProviderInput{S3IAMAction: s3IAMAction, Bucket: bucket, Key: key})
	if err != nil {
		return nil, errors.E(err, fmt.Sprintf("getting sessions from provider %T", c.provider))
	}
	clients := make([]s3iface.S3API, len(sessions))
	for i, session := range sessions {
		key := clientCacheKey{region, session}
		obj, ok := c.clients.Load(key)
		if !ok {
			obj, _ = c.clients.LoadOrStore(key, s3.New(session, &aws.Config{Region: &region}))
		}
		clients[i] = obj.(*s3.S3)
	}
	return clients, nil
}
