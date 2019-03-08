package main

import (
	"bytes"
	"context"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/grailbio/base/cmd/grail-file/cmd"
	"github.com/grailbio/base/file"
	"github.com/grailbio/testutil"
	"github.com/stretchr/testify/assert"
)

func readFile(path string) string {
	got, err := file.ReadFile(context.Background(), path)
	if err != nil {
		return err.Error()
	}
	return string(got)
}

func TestLs(t *testing.T) {
	doLs := func(args ...string) []string {
		out := bytes.Buffer{}
		assert.NoError(t, cmd.Ls(context.Background(), &out, args))
		s := strings.Split(strings.TrimSpace(out.String()), "\n")
		sort.Strings(s)
		return s
	}

	ctx := context.Background()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()

	path0 := file.Join(tmpDir, "0.txt")
	path1 := file.Join(tmpDir, "d/1.txt")
	assert.NoError(t, file.WriteFile(ctx, path0, []byte("0")))
	assert.NoError(t, file.WriteFile(ctx, path1, []byte("1")))
	assert.Equal(t,
		[]string{tmpDir + "/0.txt", tmpDir + "/d/"},
		doLs(tmpDir))
	assert.Equal(t,
		[]string{tmpDir + "/0.txt", tmpDir + "/d/1.txt"},
		doLs("-R", tmpDir))

	s := doLs("-l", tmpDir)
	assert.Equal(t, 2, len(s))
	assert.Regexp(t, tmpDir+"/0.txt\t1\t20.*", s[0])
	assert.Equal(t, tmpDir+"/d/", s[1])

	s = doLs("-l", "-R", tmpDir)
	assert.Equal(t, 2, len(s))
	assert.Regexp(t, tmpDir+"/0.txt\t1\t20.*", s[0])
	assert.Regexp(t, tmpDir+"/d/1.txt\t1\t20.*", s[1])
}

func TestCp(t *testing.T) {
	ctx := context.Background()

	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	src0Path := file.Join(tmpDir, "tmp0.txt")
	src1Path := file.Join(tmpDir, "tmp1.txt")
	expected0 := "tmp0"
	expected1 := "tmp1"
	assert.NoError(t, file.WriteFile(ctx, src0Path, []byte(expected0)))
	assert.NoError(t, file.WriteFile(ctx, src1Path, []byte(expected1)))

	// "cp xxx yyy", where yyy doesn't exist.
	dstPath := file.Join(tmpDir, "d0.txt")
	assert.NoError(t, cmd.Cp(ctx, os.Stdout, []string{src0Path, dstPath}))
	assert.Equal(t, expected0, readFile(dstPath))

	// "cp x0 x1 yyy", where yyy doesn't exist
	dstPath = file.Join(tmpDir, "d1")
	assert.NoError(t, cmd.Cp(ctx, os.Stdout, []string{src0Path, src1Path, dstPath}))
	assert.Equal(t, expected0, readFile(file.Join(dstPath, "tmp0.txt")))
	assert.Equal(t, expected1, readFile(file.Join(dstPath, "tmp1.txt")))

	// Try "cp xxx yyy/", where yyy doesn't exist. Cp should create file yyy/xxx.
	dstDir := file.Join(tmpDir, "testdir0")
	assert.NoError(t, cmd.Cp(ctx, os.Stdout, []string{src0Path, dstDir + "/"}))
	assert.Equal(t, expected0, readFile(file.Join(dstDir, "tmp0.txt")))

	dstDir = tmpDir + "/d2"
	assert.NoError(t, os.Mkdir(dstDir, 0700))
	assert.NoError(t, cmd.Cp(ctx, os.Stdout, []string{src0Path, dstDir}))
	assert.Equal(t, expected0, readFile(file.Join(dstDir, "tmp0.txt")))
}

func TestCpRecursive(t *testing.T) {
	ctx := context.Background()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()

	srcDir := file.Join(tmpDir, "dir")
	path0 := "/dir/tmp0.txt"
	path1 := "/dir/dir2/tmp1.txt"
	path2 := "/dir/dir2/dir3/tmp2.txt"
	expected0 := "tmp0"
	expected1 := "tmp1"
	expected2 := "tmp2"
	assert.NoError(t, file.WriteFile(ctx, srcDir+path0, []byte(expected0)))
	assert.NoError(t, file.WriteFile(ctx, srcDir+path1, []byte(expected1)))
	assert.NoError(t, file.WriteFile(ctx, srcDir+path2, []byte(expected2)))
	dstDir := file.Join(tmpDir, "dir1")
	assert.NoError(t, cmd.Cp(ctx, os.Stdout, []string{"-R", srcDir, dstDir}))
	assert.Equal(t, expected0, readFile(dstDir+path0))
	assert.Equal(t, expected1, readFile(dstDir+path1))
	assert.Equal(t, expected2, readFile(dstDir+path2))
}

func TestRm(t *testing.T) {
	ctx := context.Background()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	src0Path := file.Join(tmpDir, "tmp0.txt")
	src1Path := file.Join(tmpDir, "tmp1.txt")
	src2Path := file.Join(tmpDir, "tmp2.txt")
	assert.NoError(t, file.WriteFile(ctx, src0Path, []byte("0")))
	assert.NoError(t, file.WriteFile(ctx, src1Path, []byte("1")))
	assert.NoError(t, file.WriteFile(ctx, src2Path, []byte("2")))

	assert.Equal(t, "0", readFile(src0Path))
	assert.Equal(t, "1", readFile(src1Path))
	assert.Equal(t, "2", readFile(src2Path))

	assert.NoError(t, cmd.Rm(ctx, os.Stdout, []string{src0Path, src1Path}))
	assert.Regexp(t, "no such file", readFile(src0Path))
	assert.Regexp(t, "no such file", readFile(src1Path))
	assert.Equal(t, "2", readFile(src2Path))

	assert.NoError(t, cmd.Rm(ctx, os.Stdout, []string{src2Path}))
	assert.Regexp(t, "no such file", readFile(src0Path))
	assert.Regexp(t, "no such file", readFile(src1Path))
	assert.Regexp(t, "no such file", readFile(src2Path))
}

func TestRmRecursive(t *testing.T) {
	ctx := context.Background()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	src0Path := file.Join(tmpDir, "dir/tmp0.txt")
	src1Path := file.Join(tmpDir, "dir/dir2/tmp1.txt")
	src2Path := file.Join(tmpDir, "dir/dir2/dir3/tmp2.txt")
	assert.NoError(t, file.WriteFile(ctx, src0Path, []byte("0")))
	assert.NoError(t, file.WriteFile(ctx, src1Path, []byte("1")))
	assert.NoError(t, file.WriteFile(ctx, src2Path, []byte("2")))

	assert.NoError(t, cmd.Rm(ctx, os.Stdout, []string{"-R", file.Join(tmpDir, "dir/dir2")}))
	assert.Equal(t, "0", readFile(src0Path))
	assert.Regexp(t, "no such file", readFile(src1Path))
	assert.Regexp(t, "no such file", readFile(src2Path))
}
