package s3file

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
)

// List implements file.Implementation interface.
func (impl *s3Impl) List(ctx context.Context, dir string, recurse bool) file.Lister {
	scheme, bucket, key, err := ParseURL(dir)
	if err != nil {
		return &s3Lister{ctx: ctx, dir: dir, err: err}
	}
	if bucket == "" {
		if recurse {
			return &s3Lister{ctx: ctx, dir: dir,
				err: fmt.Errorf("list %s: ListBuckets cannot be combined with recurse option", dir)}
		}
		clients, clientsErr := impl.provider.Get(ctx, "ListAllMyBuckets", dir)
		if clientsErr != nil {
			return &s3Lister{ctx: ctx, dir: dir, err: clientsErr}
		}
		return &s3BucketLister{
			ctx:     ctx,
			scheme:  scheme,
			clients: clients,
		}
	}
	clients, err := impl.provider.Get(ctx, "ListBucket", dir)
	if err != nil {
		return &s3Lister{ctx: ctx, dir: dir, err: err}
	}
	return &s3Lister{
		ctx:     ctx,
		policy:  newRetryPolicy(clients, file.Opts{}),
		dir:     dir,
		scheme:  scheme,
		bucket:  bucket,
		prefix:  key,
		recurse: recurse,
	}
}

type s3Lister struct {
	ctx                         context.Context
	policy                      retryPolicy
	dir, scheme, bucket, prefix string

	object  s3Obj
	objects []s3Obj
	token   *string
	err     error
	done    bool
	recurse bool

	// consecutiveEmptyResponses counts how many times S3's ListObjectsV2WithContext returned
	// 0 records (either contents or common prefixes) consecutively.
	// Many empty responses would cause Scan to appear to hang, so we log a warning.
	consecutiveEmptyResponses int
}

type s3Obj struct {
	obj *s3.Object
	cp  *string
}

func (o s3Obj) name() string {
	if o.obj == nil {
		return *o.cp
	}
	return *o.obj.Key
}

// Scan implements Lister.Scan
func (l *s3Lister) Scan() bool {
	for {
		if l.err != nil {
			return false
		}
		l.err = l.ctx.Err()
		if l.err != nil {
			return false
		}
		if len(l.objects) > 0 {
			l.object, l.objects = l.objects[0], l.objects[1:]
			ll := len(l.prefix)
			// Ignore keys whose path component isn't exactly equal to l.prefix.  For
			// example, if l.prefix="foo/bar", then we yield "foo/bar" and
			// "foo/bar/baz", but not "foo/barbaz".
			keyVal := l.object.name()
			if ll > 0 && len(keyVal) > ll {
				if l.prefix[ll-1] == '/' {
					// Treat prefix "foo/bar/" as "foo/bar".
					ll--
				}
				if keyVal[ll] != '/' {
					continue
				}
			}
			return true
		}
		if l.done {
			return false
		}

		var prefix string
		if l.showDirs() && !strings.HasSuffix(l.prefix, pathSeparator) && l.prefix != "" {
			prefix = l.prefix + pathSeparator
		} else {
			prefix = l.prefix
		}

		req := &s3.ListObjectsV2Input{
			Bucket:            aws.String(l.bucket),
			ContinuationToken: l.token,
			Prefix:            aws.String(prefix),
		}

		if l.showDirs() {
			req.Delimiter = aws.String(pathSeparator)
		}
		var ids s3RequestIDs
		res, err := l.policy.client().ListObjectsV2WithContext(l.ctx, req, ids.captureOption())
		if l.policy.shouldRetry(l.ctx, err, l.dir) {
			continue
		}
		if err != nil {
			l.err = annotate(err, ids, &l.policy, fmt.Sprintf("s3file.list s3://%s/%s", l.bucket, l.prefix))
			return false
		}
		l.token = res.NextContinuationToken
		nRecords := len(res.Contents)
		if l.showDirs() {
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
		l.objects = make([]s3Obj, 0, nRecords)
		for _, objVal := range res.Contents {
			l.objects = append(l.objects, s3Obj{obj: objVal})
		}
		if l.showDirs() { // add the pseudo Dirs
			for _, cpVal := range res.CommonPrefixes {
				// Follow the Linux convention that directories do not come back with a trailing /
				// when read by ListDir.  To determine it is a directory, it is necessary to
				// call implementation.Stat on the path and check IsDir()
				pseudoDirName := *cpVal.Prefix
				if strings.HasSuffix(pseudoDirName, pathSeparator) {
					pseudoDirName = pseudoDirName[:len(pseudoDirName)-1]
				}
				l.objects = append(l.objects, s3Obj{cp: &pseudoDirName})
			}
		}

		l.done = !aws.BoolValue(res.IsTruncated)
	}
}

// Path implements Lister.Path
func (l *s3Lister) Path() string {
	return fmt.Sprintf("%s://%s/%s", l.scheme, l.bucket, l.object.name())
}

// Info implements Lister.Info
func (l *s3Lister) Info() file.Info {
	if obj := l.object.obj; obj != nil {
		return &s3Info{
			size:    *obj.Size,
			modTime: *obj.LastModified,
			etag:    *obj.ETag,
		}
	}
	return nil
}

// IsDir implements Lister.IsDir
func (l *s3Lister) IsDir() bool {
	return l.object.cp != nil
}

// Err returns an error, if any.
func (l *s3Lister) Err() error {
	return l.err
}

// showDirs controls whether CommonPrefixes are returned during a scan
func (l *s3Lister) showDirs() bool {
	return !l.recurse
}
