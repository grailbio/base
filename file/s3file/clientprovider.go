// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package s3file

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
)

const (
	defaultRegion     = "us-west-2"
	defaultMaxRetries = 25
)

// ClientProvider is responsible for creating an S3 client object.  Get() is
// called whenever s3File needs to access a file. The provider should cache and
// reuse the client objects, if needed. The implementation must be thread safe.
type ClientProvider interface {
	// Get returns S3 clients that can be used to perform "op" on "path".
	//
	// "op" is an S3 operation name, without the "s3:" prefix; for example
	// "PutObject" or "ListBucket". The full list of operations is defined in
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
	//
	// Path is a full URL of form "s3://bucket/key".  This method may be called
	// concurrently from multiple threads.
	//
	// Usually Get() returns one S3 client object on success. If it returns
	// multiple clients, the s3 file implementation will try each client in order,
	// until the operation succeeds.
	//
	// REQUIRES: Get returns either >=1 clients, or a non-nil error.
	Get(ctx context.Context, op, path string) ([]s3iface.S3API, error)

	// NotifyResult is called to inform that using "client" to perform "op" on
	// "path" resulted in the given error (err is nil if the op succeeded). The
	// provider should use it to optimize the list of clients to return in Get in
	// a future.
	//
	// Parameter "client" is one of the clients returned by the Get call.
	NotifyResult(ctx context.Context, op, path string, client s3iface.S3API, err error)
}

type regionCache struct {
	session *session.Session
	clients []s3iface.S3API
}

// NewDefaultProvider creates a trivial ClientProvider that uses AWS
// session.NewSession()
// (https://docs.aws.amazon.com/sdk-for-go/api/aws/session/).
//
// opts is passed to NewSession. The exception is opts.Config.Region, which will
// be be overwritten to point to the actual bucket location.
func NewDefaultProvider(opts session.Options) ClientProvider {
	region := defaultRegion
	if opts.Config.Region != nil {
		region = *opts.Config.Region
	}
	return &defaultProvider{
		opts:          opts,
		defaultRegion: region,
		regions:       make(map[string]*regionCache),
	}
}

type defaultProvider struct {
	opts          session.Options
	defaultRegion string

	mu        sync.Mutex
	regions   map[string]*regionCache
	mruRegion *regionCache
}

// GetRegion finds or creates a regionCache object for the given region.
//
// REQUIRES: p.mu is locked
func (p *defaultProvider) getRegion(region string) (*regionCache, error) {
	c, ok := p.regions[region]
	if !ok {
		opts := p.opts
		opts.Config.Region = &region
		s, err := session.NewSessionWithOptions(opts)
		if err != nil {
			return nil, err
		}
		client := s3.New(s)
		c = &regionCache{
			session: s,
			clients: []s3iface.S3API{client},
		}
		p.regions[region] = c
	}
	p.mruRegion = c
	return c, nil
}

func (p *defaultProvider) getBucketRegion(ctx context.Context, bucket string) string {
	p.mu.Lock()
	rc := p.mruRegion
	if rc == nil {
		var err error
		if rc, err = p.getRegion(p.defaultRegion); err != nil {
			log.Error.Printf("getcketregion: Failed to create client in default region %s: %v", p.defaultRegion, err)
			p.mu.Unlock()
			return p.defaultRegion
		}
	}
	p.mu.Unlock()
	region, err := GetBucketRegion(ctx, rc.clients[0], bucket)
	if err != nil {
		log.Printf("getbucketregion %s: %v. using %v", bucket, err, p.defaultRegion)
		return p.defaultRegion
	}
	return region
}

func (p *defaultProvider) Get(ctx context.Context, op, path string) ([]s3iface.S3API, error) {
	_, bucket, _, err := ParseURL(path)
	if err != nil {
		return nil, err
	}
	region := p.getBucketRegion(ctx, bucket)
	p.mu.Lock()
	c, err := p.getRegion(region)
	p.mu.Unlock()
	if err != nil {
		err = errors.E(err, fmt.Sprintf("defaultProvider.Get(%v,%s)", op, path))
	}
	return c.clients, err
}

func (p *defaultProvider) NotifyResult(ctx context.Context, op, path string, client s3iface.S3API, err error) {
}
