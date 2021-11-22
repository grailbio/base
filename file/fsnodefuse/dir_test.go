package fsnodefuse

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/grailbio/base/file/fsnode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLookupCaching checks that Lookup doesn't return stale nodes (past their cache time).
// It's a regression test.
func TestLookupCaching(t *testing.T) {
	const (
		childName = "time"
		cacheFor  = time.Millisecond
		waitSlack = 500 * time.Millisecond
	)
	// root is a directory with one child. Each time directory listing is initiated, the content
	// of the child is fixed as the current time.
	root := fsnode.NewParent(
		fsnode.NewDirInfo(""),
		fsnode.FuncChildren(func(ctx context.Context) ([]fsnode.T, error) {
			nowUnixNanos := time.Now().UnixNano()
			return []fsnode.T{
				fsnode.ConstLeaf(
					fsnode.NewRegInfo(childName).WithCacheableFor(cacheFor),
					[]byte(strconv.FormatInt(nowUnixNanos, 10)),
				),
			}, nil
		}),
	)
	withMounted(t, root, func(rootPath string) {
		childPath := path.Join(rootPath, childName)
		// Trigger a directory listing and read the event time from the file.
		listingTime := readUnixNanosFile(t, childPath)
		// Wait until that time has passed.
		// Note: We have to use wall clock time here, not mock, because we're interested in kernel
		// inode caching interactions.
		// TODO: Is there a way to guarantee that entry cache time has elapsed, for robustness?
		waitTime := listingTime.Add(waitSlack)
		sleep := waitTime.Sub(time.Now())
		time.Sleep(sleep)
		secondListingTime := readUnixNanosFile(t, childPath)
		assert.NotEqual(t,
			listingTime.UnixNano(), secondListingTime.UnixNano(),
			"second listing should have different timestamp",
		)
	})
}

func readUnixNanosFile(t *testing.T, filePath string) time.Time {
	child, err := os.Open(filePath)
	require.NoError(t, err)
	defer func() { assert.NoError(t, child.Close()) }()
	content, err := ioutil.ReadAll(child)
	require.NoError(t, err)
	listingUnixNano, err := strconv.ParseInt(string(content), 10, 64)
	require.NoError(t, err)
	return time.Unix(0, listingUnixNano)
}
