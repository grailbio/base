package s3file

import (
	"context"
	"fmt"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/addfs"
	"github.com/grailbio/base/file/fsnode"
	"github.com/grailbio/base/grail/biofs/biofseventlog"
)

type versionsFunc struct{}

var (
	VersionsFunc versionsFunc
	_            addfs.PerNodeFunc = VersionsFunc
)

func (versionsFunc) Apply(ctx context.Context, node fsnode.T) ([]fsnode.T, error) {
	biofseventlog.UsedFeature("s3.versions.func")
	// For now, we rely on gfilefs's fsnode.T implementations saving the underlying path as Sys().
	// This is temporary (BXDS-1030). When we fix that, we'll need to make this detect the
	// concrete type for S3-backed fsnode.T's instead of looking for Sys(). That'll likely require
	// refactoring such as merging gfilefs into this package.
	path, ok := node.Info().Sys().(string)
	if !ok {
		return nil, nil
	}
	scheme, bucket, key, err := ParseURL(path)
	if err != nil || scheme != Scheme {
		return nil, nil
	}
	implIface := file.FindImplementation(Scheme)
	impl, ok := implIface.(*s3Impl)
	if !ok {
		return nil, errors.E(errors.Precondition, fmt.Sprintf("unrecognized s3 impl: %T", implIface))
	}
	var (
		q   = s3Query{impl, bucket, key}
		gen fsnode.ChildrenGenerator
	)
	switch node.(type) {
	case fsnode.Parent:
		gen = versionsDirViewGen{q}
	case fsnode.Leaf:
		gen = versionsObjViewGen{q}
	default:
		return nil, errors.E(errors.Precondition, fmt.Sprintf("unrecognized node: %T", node))
	}
	return []fsnode.T{
		fsnode.NewParent(
			fsnode.NewDirInfo("versions").WithCacheableFor(fsnode.CacheableFor(node)),
			gen,
		),
	}, nil
}
