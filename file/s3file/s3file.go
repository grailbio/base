// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package s3file implements grail file interface for S3.
package s3file

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	awsrequest "github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
)

// Path separator used by s3file.
const pathSeparator = "/"

// Options defines options that can be given when creating an s3Impl
type Options struct {
	// ServerSideEncryption allows you to set the `ServerSideEncryption` value to use when
	// uploading files (e.g.  "AES256")
	ServerSideEncryption string
}

type s3Impl struct {
	provider ClientProvider
	options  Options
}

// NewImplementation creates a new file.Implementation for S3. The provider is
// called to create s3 client objects.
func NewImplementation(provider ClientProvider, opts Options) file.Implementation {
	metricAutolog()
	if *metricHTTPAddr {
		provider = metricClientProvider{provider}
	}
	return &s3Impl{provider, opts}
}

// Run handler in a separate goroutine, then wait for either the handler to
// finish, or ctx to be cancelled.
func runRequest(ctx context.Context, handler func() response) response {
	ch := make(chan response)
	go func() {
		ch <- handler()
		close(ch)
	}()
	select {
	case res := <-ch:
		return res
	case <-ctx.Done():
		return response{err: fmt.Errorf("request cancelled")}
	}
}

// String implements a human-readable description.
func (impl *s3Impl) String() string { return "s3" }

// Open opens a file for reading. The provided path should be of form
// "bucket/key..."
func (impl *s3Impl) Open(ctx context.Context, path string, opts ...file.Opts) (file.File, error) {
	f, err := impl.internalOpen(ctx, path, readonly, opts...)
	res := f.runRequest(ctx, request{reqType: statRequest})
	if res.err != nil {
		return nil, res.err
	}
	return f, err
}

// Create opens a file for writing.
func (impl *s3Impl) Create(ctx context.Context, path string, opts ...file.Opts) (file.File, error) {
	return impl.internalOpen(ctx, path, writeonly, opts...)
}

type accessMode int

const (
	readonly  accessMode = iota // file is opened by Open.
	writeonly                   // file is opened by Create.
)

func (impl *s3Impl) internalOpen(ctx context.Context, path string, mode accessMode, optsList ...file.Opts) (*s3File, error) {
	opts := mergeFileOpts(optsList)
	_, bucket, key, err := ParseURL(path)
	if err != nil {
		return nil, err
	}
	var uploader *s3Uploader
	if mode == writeonly {
		resp := runRequest(ctx, func() response {
			u, err := newUploader(ctx, impl.provider, impl.options, path, bucket, key, opts)
			return response{uploader: u, err: err}
		})
		if resp.err != nil {
			return nil, resp.err
		}
		uploader = resp.uploader
	}
	f := &s3File{
		name:     path,
		mode:     mode,
		opts:     opts,
		provider: impl.provider,
		bucket:   bucket,
		key:      key,
		uploader: uploader,
		reqCh:    make(chan request, 16),
	}
	go f.handleRequests()
	return f, err
}

// Remove implements file.Implementation interface.
func (impl *s3Impl) Remove(ctx context.Context, path string) error {
	resp := runRequest(ctx, func() response {
		_, bucket, key, err := ParseURL(path)
		if err != nil {
			return response{err: err}
		}
		clients, err := impl.provider.Get(ctx, "DeleteObject", path)
		if err != nil {
			return response{err: errors.E(err, "s3file.remove", path)}
		}
		policy := newRetryPolicy(clients, file.Opts{})
		for {
			var ids s3RequestIDs
			_, err = policy.client().DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)},
				ids.captureOption())
			if policy.shouldRetry(ctx, err, path) {
				continue
			}
			if err != nil {
				err = annotate(err, ids, &policy, "s3file.remove", path)
			}
			return response{err: err}
		}
	})
	return resp.err
}

// Presign implements file.Implementation interface.
func (impl *s3Impl) Presign(ctx context.Context, path, method string, expiry time.Duration) (string, error) {
	resp := runRequest(ctx, func() response {
		_, bucket, key, err := ParseURL(path)
		if err != nil {
			return response{err: err}
		}
		var op string
		var getRequestFn func(client s3iface.S3API) *awsrequest.Request
		switch method {
		case http.MethodGet:
			op = "GetObject"
			getRequestFn = func(client s3iface.S3API) *awsrequest.Request {
				req, _ := client.GetObjectRequest(&s3.GetObjectInput{Bucket: &bucket, Key: &key})
				return req
			}
		case http.MethodPut:
			op = "PutObject"
			getRequestFn = func(client s3iface.S3API) *awsrequest.Request {
				req, _ := client.PutObjectRequest(&s3.PutObjectInput{Bucket: &bucket, Key: &key})
				return req
			}
		case http.MethodDelete:
			op = "DeleteObject"
			getRequestFn = func(client s3iface.S3API) *awsrequest.Request {
				req, _ := client.DeleteObjectRequest(&s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
				return req
			}
		default:
			return response{err: errors.E(errors.NotSupported, "s3file.presign: unsupported http method", method)}
		}
		clients, err := impl.provider.Get(ctx, op, path)
		if err != nil {
			return response{err: err}
		}
		policy := newRetryPolicy(clients, file.Opts{})
		for {
			var ids s3RequestIDs
			req := getRequestFn(policy.client())
			req.ApplyOptions(ids.captureOption())
			url, err := req.Presign(expiry)
			if policy.shouldRetry(ctx, err, path) {
				continue
			}
			if err != nil {
				return response{err: annotate(err, ids, &policy, fmt.Sprintf("s3file.presign %s", path))}
			}
			return response{signedURL: url}
		}
	})
	return resp.signedURL, resp.err
}

// ParseURL parses a path of form "s3://grail-bucket/dir/file" and returns
// ("s3", "grail-bucket", "dir/file", nil).
func ParseURL(url string) (scheme, bucket, key string, err error) {
	var suffix string
	scheme, suffix, err = file.ParsePath(url)
	if err != nil {
		return "", "", "", err
	}
	parts := strings.SplitN(suffix, pathSeparator, 2)
	if len(parts) == 1 {
		return scheme, parts[0], "", nil
	}
	return scheme, parts[0], parts[1], nil
}

func mergeFileOpts(opts []file.Opts) (o file.Opts) {
	switch len(opts) {
	case 0:
	case 1:
		o = opts[0]
	default:
		panic(fmt.Sprintf("More than one options specified: %+v", opts))
	}
	return
}
