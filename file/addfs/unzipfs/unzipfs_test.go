package unzipfs

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/grailbio/base/file/fsnode"
	. "github.com/grailbio/base/file/fsnode/fsnodetesting"
	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/base/ioctx/fsctx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParent(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Unix(1_600_000_000, 0)

	var zipBytes bytes.Buffer
	zipW := zip.NewWriter(&zipBytes)

	a0Info := fsnode.NewRegInfo("0.txt").WithModTime(baseTime).WithModePerm(0600)
	a0Content := "a0"
	addFile(t, zipW, "a/", &a0Info, a0Content, true)

	a00Info := fsnode.NewRegInfo("0.exe").WithModTime(baseTime.Add(time.Hour)).WithModePerm(0755)
	a00Content := "a00"
	addFile(t, zipW, "a/0/", &a00Info, a00Content, true)

	b0Info := fsnode.NewRegInfo("0.txt").WithModTime(baseTime.Add(2 * time.Hour)).WithModePerm(0644)
	b0Content := "b0"
	addFile(t, zipW, "b/", &b0Info, b0Content, false)

	topInfo := fsnode.NewRegInfo("0.txt").WithModTime(baseTime.Add(3 * time.Hour)).WithModePerm(0600)
	topContent := "top"
	addFile(t, zipW, "", &topInfo, topContent, false)

	require.NoError(t, zipW.Close())

	parentInfo := fsnode.NewDirInfo("unzip")
	parent, err := parentFromLeaf(ctx, parentInfo, fsnode.ConstLeaf(fsnode.NewRegInfo("zip"), zipBytes.Bytes()))
	require.NotNil(t, parent)
	require.NoError(t, err)

	walker := Walker{Info: true}
	diff := cmp.Diff(
		InfoT{parentInfo, Parent{
			"a": InfoT{
				fsnode.NewDirInfo("a"),
				Parent{
					a0Info.Name(): InfoT{a0Info, Leaf([]byte(a0Content))},
					"0": InfoT{
						fsnode.NewDirInfo("0"),
						Parent{
							a00Info.Name(): InfoT{a00Info, Leaf([]byte(a00Content))},
						},
					},
				},
			},
			"b": InfoT{
				fsnode.NewDirInfo("b"),
				Parent{
					b0Info.Name(): InfoT{b0Info, Leaf([]byte(b0Content))},
				},
			},
			topInfo.Name(): InfoT{topInfo, Leaf([]byte(topContent))},
		}},
		walker.WalkContents(ctx, t, parent),
		cmp.Comparer(func(a, b fsnode.FileInfo) bool {
			a, b = a.WithSys(nil), b.WithSys(nil)
			return a.Equal(b)
		}),
	)
	assert.Empty(t, diff)
}

func addFile(t *testing.T, zipW *zip.Writer, prefix string, info *fsnode.FileInfo, content string, flate bool) {
	*info = info.WithSize(int64(len(content)))
	hdr, err := zip.FileInfoHeader(*info)
	hdr.Name = prefix + info.Name()
	if flate {
		hdr.Method = zip.Deflate
	}
	require.NoError(t, err)
	fw, err := zipW.CreateHeader(hdr)
	require.NoError(t, err)
	_, err = io.Copy(fw, strings.NewReader(content))
	require.NoError(t, err)
}

func TestNonZip(t *testing.T) {
	ctx := context.Background()
	parent, err := parentFromLeaf(ctx,
		fsnode.NewDirInfo("unzip"),
		fsnode.ConstLeaf(fsnode.NewRegInfo("zip"), []byte("not zip")))
	require.NoError(t, err)
	require.Nil(t, parent)
}

func TestReadCancel(t *testing.T) {
	ctx := context.Background()

	var zipBytes bytes.Buffer
	zipW := zip.NewWriter(&zipBytes)

	fInfo := fsnode.NewRegInfo("f.txt")
	// We need to make sure our reads below will exceed internal buffer sizes so we can control
	// underlying blocking. Empirically this seems big enough but it may need to increase if
	// there are internal changes (in flate, etc.) in the future.
	fContent := strings.Repeat("a", 50*1024*1024)
	addFile(t, zipW, "", &fInfo, fContent, true)

	require.NoError(t, zipW.Close())

	// First we allow unblocked reads for zip headers.
	zipLeaf := pausingLeaf{Leaf: fsnode.ConstLeaf(fsnode.NewRegInfo("zip"), zipBytes.Bytes())}
	parent, err := parentFromLeaf(ctx, fsnode.NewDirInfo("unzip"), &zipLeaf)
	require.NoError(t, err)
	require.NotNil(t, parent)
	children, err := fsnode.IterateAll(ctx, parent.Children())
	require.NoError(t, err)
	require.Equal(t, 1, len(children))
	fLeaf := children[0].(fsnode.Leaf)

	f, err := fsnode.Open(ctx, fLeaf)
	require.NoError(t, err)

	// Set up read blocking.
	waitC := make(chan struct{})
	zipLeaf.mu.Lock()
	zipLeaf.readAtWaitTwiceC = waitC
	zipLeaf.mu.Unlock()

	var n int
	b := make([]byte, 2)
	readC := make(chan struct{})
	go func() {
		defer close(readC)
		n, err = f.Read(ctx, b)
	}()
	waitC <- struct{}{} // Let the read go through.
	waitC <- struct{}{}
	<-readC
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, fContent[:2], string(b))

	// Now start another read and let it reach ReadAt (first waitC send).
	ctxCancel, cancel := context.WithCancel(ctx)
	readC = make(chan struct{})
	go func() {
		defer close(readC)
		// Make sure this read will exhaust internal buffers (in flate, etc.) forcing a read from
		// the pausingFile we control.
		_, err = io.ReadAll(ioctx.ToStdReader(ctxCancel, f))
	}()
	waitC <- struct{}{}
	cancel() // Cancel the context instead of letting the read finish.
	<-readC
	// Make sure we get a cancellation error.
	require.ErrorIs(t, err, context.Canceled)
}

// pausingLeaf returns Files (from Open) that read one item C (which may block) at the start of
// each ReadAt operation. If C is nil, ReadAt's don't block.
type pausingLeaf struct {
	fsnode.Leaf
	mu sync.Mutex // mu guards readAtWaitTwiceC
	// readAtWaitTwiceC controls ReadAt's blocking. If non-nil, ReadAt will read two values from
	// this channel before returning.
	readAtWaitTwiceC <-chan struct{}
}

func (*pausingLeaf) FSNodeT() {}
func (p *pausingLeaf) OpenFile(ctx context.Context, flag int) (fsctx.File, error) {
	f, err := fsnode.Open(ctx, p.Leaf)
	return pausingFile{p, f}, err
}

type pausingFile struct {
	leaf *pausingLeaf
	fsctx.File
}

func (p pausingFile) ReadAt(ctx context.Context, dst []byte, off int64) (n int, err error) {
	p.leaf.mu.Lock()
	waitC := p.leaf.readAtWaitTwiceC
	p.leaf.mu.Unlock()
	if waitC != nil {
		for i := 0; i < 2; i++ {
			log.Printf("pausing: waiting %d", i)
			select {
			case <-waitC:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
	} else {
		log.Printf("pausing: nil")
	}
	return p.File.(ioctx.ReaderAt).ReadAt(ctx, dst, off)
}
