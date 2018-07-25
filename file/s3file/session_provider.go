// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package s3file

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
)

const (
	defaultRegion                        = "us-west-2"
	clientCacheGarbageCollectionInterval = 10 * time.Minute
)

type (
	// SessionProvider provides Sessions for making AWS API calls. Get() is called whenever s3file
	// needs to access a file. The provider should cache and reuse the sessions, if needed.
	// The implementation must be thread safe.
	SessionProvider interface {
		// Get returns AWS sessions that can be used to perform in.S3IAMAction on
		// s3://{in.bucket}/{in.key}.
		//
		// s3file maintains an internal cache keyed by *session.Session that is only pruned
		// occasionally. Get() is called for every S3 operation so it should be very fast. Caching
		// (that is, reusing *session.Session whenever possible) is strongly encouraged.
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
		// clients maps clientCacheKey -> *clientCacheValue.
		// TODO: Implement some kind of garbage collection and relax the documented constraint
		// that sessions are never released.
		clients *sync.Map
	}
	clientCacheKey struct {
		region string
		// userSession is the session that the user's SessionProvider returned.
		// It may be configured for a different region, so we don't use it directly.
		userSession *session.Session
	}
	clientCacheValue struct {
		client *s3.S3
		// usedSinceLastGC is 0 or 1. It's set when this client is used, and acted on by the
		// GC goroutine.
		// TODO: Use atomic.Bool in go1.19.
		usedSinceLastGC int32
	}
)

func newClientCache(provider SessionProvider) *clientCache {
	// According to time.Tick documentation, ticker.Stop must be called to avoid leaking ticker
	// memory. However, *clientCache is never explicitly "shut down", so we don't have a good way
	// to stop the GC loop. Instead, we use a finalizer on *clientCache, and ensure the GC loop
	// itself doesn't keep *clientCache alive.
	var (
		clients         sync.Map
		gcCtx, gcCancel = context.WithCancel(context.Background())
	)
	go func() {
		ticker := time.NewTicker(clientCacheGarbageCollectionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-gcCtx.Done():
				return
			case <-ticker.C:
			}
			clients.Range(func(keyAny, valueAny any) bool {
				key := keyAny.(clientCacheKey)
				value := valueAny.(*clientCacheValue)
				if atomic.SwapInt32(&value.usedSinceLastGC, 0) == 0 {
					// Note: Concurrent goroutines could mark this client as used between our query
					// and delete. That's fine; we'll just construct a new client next time.
					clients.Delete(key)
				}
				return true
			})
		}
	}()
	// Note: Declare *clientCache after the GC loop to help ensure the latter doesn't keep a
	// reference to the former.
	cc := clientCache{provider, &clients}
	runtime.SetFinalizer(&cc, func(any) { gcCancel() })
	return &cc
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
			obj, _ = c.clients.LoadOrStore(key, &clientCacheValue{
				client:          s3.New(session, &aws.Config{Region: &region}),
				usedSinceLastGC: 1,
			})
		}
		value := obj.(*clientCacheValue)
		clients[i] = value.client
		atomic.StoreInt32(&value.usedSinceLastGC, 1)
	}
	return clients, nil
}
