// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package file_test

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/grailbio/base/file"
	filetestutil "github.com/grailbio/base/file/internal/testutil"
	"github.com/grailbio/testutil"
	"github.com/stretchr/testify/require"
)

func TestAll(t *testing.T) {
	tempDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	impl := file.NewLocalImplementation()
	ctx := context.Background()
	filetestutil.TestAll(ctx, t, impl, tempDir)
}

func TestEmptyPath(t *testing.T) {
	_, err := file.Create(context.Background(), "")
	require.Regexp(t, "empty pathname", err)
}

// Test that Create on a symlink will preserve it.
func TestCreateSymlink(t *testing.T) {
	dir0, cleanup0 := testutil.TempDir(t, "", "")
	dir1, cleanup1 := testutil.TempDir(t, "", "")
	defer cleanup1()
	defer cleanup0()

	newPath := filepath.Join(dir1, "new")
	oldPath := filepath.Join(dir0, "old")
	require.NoError(t, os.Symlink(oldPath, newPath))
	require.NoError(t, ioutil.WriteFile(oldPath, []byte("hoofah"), 0777))

	ctx := context.Background()
	w, err := file.Create(context.Background(), newPath)
	require.NoError(t, err)
	w.Writer(ctx).Write([]byte("hello"))
	require.NoError(t, w.Close(ctx))

	data, err := ioutil.ReadFile(newPath)
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))

	// The file should have been created in the symlink dest dir.
	data, err = ioutil.ReadFile(oldPath)
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))
}
