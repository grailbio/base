package s3file

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/x/parlist"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
)

// List implements file.Implementation interface.
func (impl *s3Impl) List(ctx context.Context, dir string, recurse bool) file.Lister {
	batch := impl.ListBatch(ctx, dir, parlist.ListOpts{Recursive: recurse})
	return parlist.NewAdapter(ctx, batch)
}

var _ parlist.Implementation = (*s3Impl)(nil)

// ListOpts implements parlist.Implementation. It's experimental and should not be used directly.
func (impl *s3Impl) ListBatch(ctx context.Context, dir string, opts parlist.ListOpts) parlist.BatchLister {
	scheme, bucket, key, err := ParseURL(dir)
	if err != nil {
		return &s3BatchLister{dir: dir, err: err}
	}
	if bucket == "" {
		if opts.Recursive {
			return &s3BatchLister{dir: dir,
				err: fmt.Errorf("list %s: ListBuckets cannot be combined with recurse option", dir)}
		}
		clients, clientsErr := impl.provider.Get(ctx, "ListAllMyBuckets", dir)
		if clientsErr != nil {
			return &s3BatchLister{dir: dir, err: clientsErr}
		}
		return &s3BucketLister{
			scheme:  scheme,
			clients: clients,
		}
	}
	clients, err := impl.provider.Get(ctx, "ListBucket", dir)
	if err != nil {
		return &s3BatchLister{dir: dir, err: err}
	}
	return &s3BatchLister{
		policy: newRetryPolicy(clients, file.Opts{}),
		dir:    dir,
		scheme: scheme,
		bucket: bucket,
		prefix: key,
		opts:   opts,
	}
}

type s3BatchLister struct {
	policy retryPolicy
	// TODO(josh): Remove dir.
	dir, scheme, bucket, prefix string
	opts                        parlist.ListOpts

	token *string
	err   error

	// consecutiveEmptyResponses counts how many times S3's ListObjectsV2WithContext returned
	// 0 records (either contents or common prefixes) consecutively.
	// Many empty responses would cause Scan to appear to hang, so we log a warning.
	consecutiveEmptyResponses int
}

var _ parlist.BatchLister = (*s3BatchLister)(nil)

// Scan implements Lister.Scan
func (l *s3BatchLister) Scan(ctx context.Context) (batch []parlist.Info, more bool) {
	// TODO: Use a for-loop just around the request retry rather than the whole body.
	for {
		if l.err != nil {
			return nil, false
		}

		var (
			prefix   string
			showDirs = !l.opts.Recursive
		)
		if showDirs && !strings.HasSuffix(l.prefix, pathSeparator) && l.prefix != "" {
			prefix = l.prefix + pathSeparator
		} else {
			prefix = l.prefix
		}

		req := &s3.ListObjectsV2Input{
			Bucket:            aws.String(l.bucket),
			ContinuationToken: l.token,
			Prefix:            aws.String(prefix),
		}
		if l.opts.StartAfter != "" {
			if len(prefix) == 0 || prefix[len(prefix)-1:] == pathSeparator {
				req.StartAfter = aws.String(prefix + l.opts.StartAfter)
			} else {
				req.StartAfter = aws.String(prefix + pathSeparator + l.opts.StartAfter)
			}
		}
		if l.opts.BatchSizeHint > 0 {
			req.MaxKeys = aws.Int64(int64(l.opts.BatchSizeHint))
		}
		if showDirs {
			req.Delimiter = aws.String(pathSeparator)
		}
		var ids s3RequestIDs
		res, err := l.policy.client().ListObjectsV2WithContext(ctx, req, ids.captureOption())
		if l.policy.shouldRetry(ctx, err, l.dir) {
			continue
		}
		if err != nil {
			l.err = annotate(err, ids, &l.policy, fmt.Sprintf("s3file.list s3://%s/%s", l.bucket, l.prefix))
			return nil, false
		}
		l.token = res.NextContinuationToken
		nRecords := len(res.Contents)
		if showDirs {
			nRecords += len(res.CommonPrefixes)
		}
		if nRecords > 0 {
			l.consecutiveEmptyResponses = 0
		} else {
			l.consecutiveEmptyResponses++
			if n := l.consecutiveEmptyResponses; n > 7 && n&(n-1) == 0 {
				log.Printf("s3file.list.scan: warning: S3 returned empty response %d consecutive times", n)
			}
		}

		batch = make([]parlist.Info, 0, nRecords)
		for _, obj := range res.Contents {
			relPath := *obj.Key
			if !l.keepObj(relPath) {
				continue
			}
			batch = append(batch, s3Obj{
				path:    fmt.Sprintf("%s://%s/%s", l.scheme, l.bucket, relPath),
				size:    *obj.Size,
				modTime: *obj.LastModified,
				eTag:    *obj.ETag,
			})
		}

		if showDirs {
			for _, cpVal := range res.CommonPrefixes {
				// Follow the Linux convention that directories do not come back with a trailing /
				// when read by ListDir.
				pseudoDirName := *cpVal.Prefix
				if !l.keepObj(pseudoDirName) {
					continue
				}
				if strings.HasSuffix(pseudoDirName, pathSeparator) {
					pseudoDirName = pseudoDirName[:len(pseudoDirName)-1]
				}
				batch = append(batch, s3Obj{
					path:  fmt.Sprintf("%s://%s/%s", l.scheme, l.bucket, pseudoDirName),
					isDir: true,
				})
			}
		}

		return batch, aws.BoolValue(res.IsTruncated)
	}
}

// keepObj skips keys whose path component isn't exactly equal to l.prefix.  For
// example, if l.prefix == "foo/bar", then we yield "foo/bar" and
// "foo/bar/baz", but not "foo/barbaz".
func (l *s3BatchLister) keepObj(objPath string) bool {
	ll := len(l.prefix)
	must.Truef(l.prefix == objPath[:ll], "%s, %s", l.prefix, objPath)
	if ll > 0 && len(objPath) > ll {
		if l.prefix[ll-1] == '/' {
			// Treat prefix "foo/bar/" as "foo/bar".
			ll--
		}
		if objPath[ll] != '/' {
			return false
		}
	}
	return true
}

func (l *s3BatchLister) Err() error { return l.err }

type s3Obj struct {
	path    string
	isDir   bool
	size    int64
	modTime time.Time
	eTag    string
}

func (o s3Obj) Path() string       { return o.path }
func (o s3Obj) IsDir() bool        { return o.isDir }
func (o s3Obj) Size() int64        { return o.size }
func (o s3Obj) ModTime() time.Time { return o.modTime }
func (o s3Obj) ETag() string       { return o.eTag }
