package parlist_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/internal/testutil"
	"github.com/grailbio/base/file/s3file"
	"github.com/grailbio/base/file/x/parlist"
	"github.com/grailbio/testutil/s3test"
)

func TestS3(t *testing.T) {
	provider := &testProvider{clients: []s3iface.S3API{s3test.NewClient(t, "b")}}
	ctx := context.Background()
	s3Impl := s3file.NewImplementation(provider, s3file.Options{})
	impl := testImpl{
		Implementation: s3Impl,
		EmbeddedImpl:   parlist.EmbeddedImpl{s3Impl.(parlist.Implementation)},
	}
	testutil.TestAll(ctx, t, impl, "s3://b/dir")
}

type testImpl struct {
	file.Implementation
	parlist.EmbeddedImpl
}

func (impl testImpl) List(ctx context.Context, path string, recursive bool) file.Lister {
	shardLister := impl.ParList(ctx, path, parlist.ListOpts{Recursive: recursive})
	batchLister := shardLister.NewShard()
	return parlist.NewAdapter(ctx, batchLister)
}

type testProvider struct {
	clients []s3iface.S3API
}

func (p *testProvider) Get(context.Context, string, string) ([]s3iface.S3API, error) {
	return p.clients, nil
}
func (p *testProvider) NotifyResult(context.Context, string, string, s3iface.S3API, error) {}
