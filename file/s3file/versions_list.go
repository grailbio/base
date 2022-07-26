package s3file

import (
	"context"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/grail/biofs/biofseventlog"
	"github.com/grailbio/base/must"
)

// s3Query is a generic description of an S3 object or prefix.
type s3Query struct {
	impl *s3Impl
	// bucket must be non-empty.
	bucket string
	// key is either an S3 object's key or a key prefix (optionally ending with pathSeparator).
	// "" is allowed and refers to the root of the bucket.
	key string
}

func (q s3Query) path() string { return pathPrefix + q.bucket + pathSeparator + q.key }

// TODO: Dedupe with gfilefs.
const fileInfoCacheFor = 1 * time.Hour

type (
	// versionsDirViewGen lists all the versions of all the (direct child) objects in a single S3
	// "directory" (that is, single bucket with a single key prefix).
	//
	// Note: We implement fsnode.ChildrenGenerator rather than fsnode.Iterator because it reduces
	// implementation complexity. We need to parse three separate fields from listing responses
	// so the implementation is a bit verbose, and Child()/Children() differences introduce edge
	// cases we should test. But, we'll probably want to do this eventually.
	versionsDirViewGen struct{ s3Query }

	// versionsObjectGen lists the versions of an S3 object. Each version of the object is accessible
	// via a child node. Additionally, if there are other S3 object versions that have this path as
	// a prefix (or, in directory terms, if there used to be a directory with the same name as this
	// file), a dir/ child provides access to those.
	//
	// Scheme:
	//   vVERSION_ID/ for each version
	//   vVERSION_ID (empty file) to mark deletion time
	//   dir/ for children, if there used to be a "directory" with this name
	// TODO:
	//   @DATE/ -> VERSION_ID/ for each version
	//   latest/ -> VERSION_ID/
	//   0/, 1/, etc. -> VERSION_ID/
	//
	// Note: We implement fsnode.ChildrenGenerator rather than fsnode.Iterator because it reduces
	// implementation complexity and we expect number of versions per object to be relatively
	// modest in practice. If we see performance problems, we can make it more sophisticated.
	versionsObjViewGen struct{ s3Query }
)

var (
	_ fsnode.ChildrenGenerator = versionsDirViewGen{}
	_ fsnode.ChildrenGenerator = versionsObjViewGen{}

	objViewDirInfo = fsnode.NewDirInfo("dir").WithCacheableFor(fileInfoCacheFor)
)

func (g versionsDirViewGen) GenerateChildren(ctx context.Context) ([]fsnode.T, error) {
	biofseventlog.UsedFeature("s3.versions.dirview")
	dirPrefix := g.key
	if dirPrefix != "" {
		dirPrefix = g.key + pathSeparator
	}
	iterator, err := newVersionsIterator(ctx, g.impl, g.path(), s3.ListObjectVersionsInput{
		Bucket:    aws.String(g.bucket),
		Delimiter: aws.String(pathSeparator),
		Prefix:    aws.String(dirPrefix),
	})
	if err != nil {
		return nil, err
	}
	var (
		dirChildren = map[string]fsnode.T{}
		objChildren = map[string][]fsnode.T{}
	)
	for iterator.HasNextPage() {
		out, err := iterator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, common := range out.CommonPrefixes {
			name := (*common.Prefix)[len(dirPrefix):]
			name = name[:len(name)-len(pathSeparator)]
			if name == "" {
				// Note: S3 keys may have multiple trailing `/`s leading to name == "".
				// For now, we skip these, making them inaccessible to users.
				// TODO: Better mapping of S3 key semantics onto fsnode.T, for example recursively
				// listing "key//" so we can merge those children into "key/"'s.
				// See also: BXDS-2039 for the non-version listing case.
				continue
			}
			q := g.s3Query
			q.key = dirPrefix + name
			dirChildren[name] = fsnode.NewParent(
				fsnode.NewDirInfo(name).WithCacheableFor(fileInfoCacheFor),
				versionsDirViewGen{q})
		}
		for _, del := range out.DeleteMarkers {
			if *del.Key == dirPrefix {
				continue // Skip directory markers.
			}
			name := (*del.Key)[len(dirPrefix):]
			objChildren[name] = append(objChildren[name], newDeleteChild(del))
		}
		for _, version := range out.Versions {
			if *version.Key == dirPrefix {
				continue // Skip directory markers.
			}
			q := g.s3Query
			q.key = *version.Key
			name := q.key[len(dirPrefix):]
			objChildren[name] = append(objChildren[name], newVersionChild(q, version))
		}
	}
	merged := make([]fsnode.T, 0, len(dirChildren)+len(objChildren))
	for name, child := range dirChildren {
		if _, ok := objChildren[name]; ok {
			// If a name was used both for files and directories, prefer files here, because
			// the user can find the directory view under {name}/dir/.
			continue
		}
		merged = append(merged, child)
	}
	for name, children := range objChildren {
		merged = append(merged, fsnode.NewParent(
			fsnode.NewDirInfo(name).WithCacheableFor(fileInfoCacheFor),
			fsnode.ConstChildren(children...),
		))
	}
	return merged, nil
}

func (g versionsObjViewGen) GenerateChildren(ctx context.Context) ([]fsnode.T, error) {
	biofseventlog.UsedFeature("s3.versions.objview")
	iterator, err := newVersionsIterator(ctx, g.impl, g.path(), s3.ListObjectVersionsInput{
		Bucket:    aws.String(g.bucket),
		Delimiter: aws.String(pathSeparator),
		Prefix:    aws.String(g.key),
	})
	if err != nil {
		return nil, err
	}
	var (
		versions         []fsnode.T
		hasOtherChildren bool
	)
	for iterator.HasNextPage() {
		out, err := iterator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		if len(out.CommonPrefixes) > 0 {
			hasOtherChildren = true
		}
		for _, del := range out.DeleteMarkers {
			if *del.Key != g.key {
				hasOtherChildren = true
				// del is in a "subdirectory" of a previous directory version of our object.
				// We don't render those here; instead we just add the dir/ child below.
				continue
				// Note: It seems like S3 returns delete markers in sorted order, but the API
				// docs don't explicitly state this for ListObjectVersions [1] as they do for
				// ListObjectsV2 [2], so we `continue` instead of `break`. We're still assuming
				// API response pages are so ordered, though, because the alternative is unworkable.
				// TODO: Ask AWS for explicit documentation on versions ordering.
				//
				// [1] https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
				// [2] https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
			}
			versions = append(versions, newDeleteChild(del))
		}
		for _, version := range out.Versions {
			if *version.Key != g.key {
				hasOtherChildren = true
				continue // See delete marker note.
			}
			versions = append(versions, newVersionChild(g.s3Query, version))
		}
	}
	if hasOtherChildren {
		versions = append(versions, fsnode.NewParent(objViewDirInfo, versionsDirViewGen{g.s3Query}))
	}
	return versions, nil
}

func newVersionChild(q s3Query, v *s3.ObjectVersion) fsnode.Parent {
	must.Truef(len(q.key) > 0, "creating child for %#v, %v", q, v)
	name := q.key
	if idx := strings.LastIndex(name, pathSeparator); idx >= 0 {
		name = name[idx+len(pathSeparator):]
	}
	dirName := "v" + sanitizePathElem(*v.VersionId)
	// Some S3 storage classes don't allow immediate, direct access (for example, requiring restore
	// first). They also have very different cost profiles and users may not know about these and
	// accidentally, expensively download many objects (especially with biofs, where it's easy
	// to run `grep`, etc.). We have a best-effort allowlist and block others for now.
	// TODO: Refine this UX. Maybe add a README.txt describing these properties and suggesting
	// using the AWS console for unsupported objects.
	// TODO: Consider supporting Glacier restoration.
	//
	// Note: This field's `enum` tag [1] names an enum with only one value (standard class [2]).
	// However, as of this writing, we're seeing the API return more values, like DEEP_ARCHIVE.
	// We assume it can take any value in s3.ObjectStorageClass* instead.
	// TODO: Verify this, report to AWS, etc.
	// [1] https://pkg.go.dev/github.com/aws/aws-sdk-go@v1.42.0/service/s3#ObjectVersion.StorageClass
	// [2] https://pkg.go.dev/github.com/aws/aws-sdk-go@v1.42.0/service/s3#ObjectVersionStorageClassStandard
	switch *v.StorageClass {
	default:
		dirName += "." + *v.StorageClass
		return fsnode.NewParent(
			fsnode.NewDirInfo(dirName).WithModTime(*v.LastModified),
			fsnode.ConstChildren())
	case
		s3.ObjectStorageClassStandard,
		s3.ObjectStorageClassReducedRedundancy,
		s3.ObjectStorageClassStandardIa,
		s3.ObjectStorageClassOnezoneIa,
		s3.ObjectStorageClassIntelligentTiering:
		return fsnode.NewParent(
			fsnode.NewDirInfo(dirName).WithModTime(*v.LastModified),
			fsnode.ConstChildren(
				versionsLeaf{
					FileInfo:  fsnode.NewRegInfo(name).WithSize(*v.Size).WithModTime(*v.LastModified),
					s3Query:   q,
					versionID: *v.VersionId,
				},
			),
		)
	}
}

func newDeleteChild(del *s3.DeleteMarkerEntry) fsnode.T {
	return fsnode.ConstLeaf(
		fsnode.NewRegInfo("v"+sanitizePathElem(*del.VersionId)).WithModTime(*del.LastModified),
		nil)
}

type versionsIterator struct {
	in     s3.ListObjectVersionsInput
	eof    bool
	policy retryPolicy
	path   string
}

func newVersionsIterator(
	ctx context.Context,
	impl *s3Impl,
	path string,
	in s3.ListObjectVersionsInput,
) (*versionsIterator, error) {
	clients, err := impl.provider.Get(ctx, "ListVersions", path)
	if err != nil {
		return nil, errors.E(err, "getting clients")
	}
	policy := newBackoffPolicy(clients, file.Opts{})
	return &versionsIterator{in: in, policy: policy, path: path}, nil
}

func (it *versionsIterator) HasNextPage() bool { return !it.eof }

func (it *versionsIterator) NextPage(ctx context.Context) (*s3.ListObjectVersionsOutput, error) {
	for {
		var ids s3RequestIDs
		out, err := it.policy.client().ListObjectVersionsWithContext(ctx, &it.in, ids.captureOption())
		if err == nil {
			it.in.KeyMarker = out.NextKeyMarker
			it.in.VersionIdMarker = out.NextVersionIdMarker
			if !*out.IsTruncated {
				it.eof = true
			}
			return out, nil
		}
		if !it.policy.shouldRetry(ctx, err, it.path) {
			it.eof = true
			return nil, annotate(err, ids, &it.policy, "s3file.versionsRootNode.Child", it.path)
		}
	}
}

func sanitizePathElem(s string) string {
	// TODO: Consider being stricter. S3 guarantees very little about version IDs:
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/versioning-workflows.html#version-ids
	// TODO: Implement more robust replacement (with some escape char, etc.) so that we cannot
	// introduce collisions.
	return strings.ReplaceAll(s, "/", "_")
}
