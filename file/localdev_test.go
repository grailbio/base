package file_test

import (
	"context"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/grailbio/base/file"
	"github.com/grailbio/testutil"
	"github.com/stretchr/testify/require"
)

// Write to /dev/stdout. This test only checks that the write succeeds.
func TestStdout(t *testing.T) {
	var err error

	if testing.Short() {
		t.Skip("Cannot open /dev/tty or /dev/stdout from automated tests")
	}

	for _, path := range []string{
		"/dev/tty",    // works on darwin
		"/dev/stdout", // works on linux
	} {
		ctx := context.Background()
		var w file.File
		w, err = file.Create(ctx, path)
		if err != nil {
			continue
		}
		_, err = w.Writer(ctx).Write([]byte("Hello"))
		if err != nil {
			continue
		}
		require.NoError(t, w.Close(ctx))
		break
	}
	require.NoError(t, err)
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
		w.Writer(ctx).Write([]byte("Hello\n"))
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
