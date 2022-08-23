//+build !unit

package file_test

import (
	"context"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/grailbio/base/file"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/assert"
	"github.com/stretchr/testify/require"
)

// Write to /dev/stdout. This test only checks that the write succeeds.
func TestStdout(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("This test does not consistently work on macOS")
	}
	ctx := context.Background()
	w, err := file.Create(ctx, "/dev/stdout")
	assert.Nil(t, err)
	_, err = w.Writer(ctx).Write([]byte("Hello\n"))
	assert.Nil(t, err)
	require.NoError(t, w.Close(ctx))
}

// Read and write a FIFO.
func TestDevice(t *testing.T) {
	mkfifo, err := exec.LookPath("mkfifo")
	if err != nil {
		t.Skipf("mkfifo not found, skipping the test: %v", err)
	}
	tempDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()

	fifoPath := filepath.Join(tempDir, "fifo")
	require.NoError(t, exec.Command(mkfifo, fifoPath).Run())

	ctx := context.Background()
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		w, err := file.Create(ctx, fifoPath)
		require.NoError(t, err)
		_, err = w.Writer(ctx).Write([]byte("Hello\n"))
		require.NoError(t, err)
		require.NoError(t, w.Close(ctx))
		wg.Done()
	}()

	var data []byte
	go func() {
		r, err := file.Open(ctx, fifoPath)
		require.NoError(t, err)
		data, err = ioutil.ReadAll(r.Reader(ctx))
		require.NoError(t, err)
		wg.Done()
	}()
	wg.Wait()
	require.Equal(t, "Hello\n", string(data))
}
