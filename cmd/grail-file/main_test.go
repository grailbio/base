package main

import (
	"bytes"
	"context"
	"os"
	"sort"
	"strings"
	"testing"

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
	doLs := func(dir string, opts lsOpts) []string {
		out := bytes.Buffer{}
		assert.NoError(t, runLs(&out, []string{dir}, opts))
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
		doLs(tmpDir, lsOpts{}))
	assert.Equal(t,
		[]string{tmpDir + "/0.txt", tmpDir + "/d/1.txt"},
		doLs(tmpDir, lsOpts{recursive: true}))

	s := doLs(tmpDir, lsOpts{longOutput: true})
	assert.Equal(t, 2, len(s))
	assert.Regexp(t, tmpDir+"/0.txt\t1\t20.*", s[0])
	assert.Equal(t, tmpDir+"/d/", s[1])

	s = doLs(tmpDir, lsOpts{longOutput: true, recursive: true})
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
	assert.NoError(t, runCp([]string{src0Path, dstPath}, cprmOpts{}))
	assert.Equal(t, expected0, readFile(dstPath))

	// "cp x0 x1 yyy", where yyy doesn't exist
	dstPath = file.Join(tmpDir, "d1")
	assert.NoError(t, runCp([]string{src0Path, src1Path, dstPath}, cprmOpts{}))
	assert.Equal(t, expected0, readFile(file.Join(dstPath, "tmp0.txt")))
	assert.Equal(t, expected1, readFile(file.Join(dstPath, "tmp1.txt")))

	// Try "cp xxx yyy/", where yyy doesn't exist. Cp should create file yyy/xxx.
	dstDir := file.Join(tmpDir, "testdir0")
	assert.NoError(t, runCp([]string{src0Path, dstDir + "/"}, cprmOpts{}))
	assert.Equal(t, expected0, readFile(file.Join(dstDir, "tmp0.txt")))

	dstDir = tmpDir + "/d2"
	assert.NoError(t, os.Mkdir(dstDir, 0700))
	assert.NoError(t, runCp([]string{src0Path, dstDir}, cprmOpts{}))
	assert.Equal(t, expected0, readFile(file.Join(dstDir, "tmp0.txt")))
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

	assert.NoError(t, runRm([]string{src0Path, src1Path}, cprmOpts{}))
	assert.Regexp(t, "no such file", readFile(src0Path))
	assert.Regexp(t, "no such file", readFile(src1Path))
	assert.Equal(t, "2", readFile(src2Path))

	assert.NoError(t, runRm([]string{src2Path}, cprmOpts{}))
	assert.Regexp(t, "no such file", readFile(src0Path))
	assert.Regexp(t, "no such file", readFile(src1Path))
	assert.Regexp(t, "no such file", readFile(src2Path))
}

func TestParseGlob(t *testing.T) {
	doParse := func(str string) string {
		prefix, hasGlob := parseGlob(str)
		if !hasGlob {
			return "none"
		}
		return prefix
	}
	assert.Equal(t, "none", doParse("s3://a/b/c"))
	assert.Equal(t, "none", doParse("s3://a/b\\*/c"))
	assert.Equal(t, "s3://a/", doParse("s3://a/b*/c"))
	assert.Equal(t, "s3://a/b/", doParse("s3://a/b/*"))
	assert.Equal(t, "s3://a/", doParse("s3://a/b?"))
	assert.Equal(t, "s3://a/", doParse("s3://a/**/b"))
	assert.Equal(t, "", doParse("**"))
}

func TestExpandGlob(t *testing.T) {
	ctx := context.Background()
	tmpDir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	src0Path := file.Join(tmpDir, "abc/def/tmp0")
	src1Path := file.Join(tmpDir, "abd/efg/hij/tmp1")
	src2Path := file.Join(tmpDir, "tmp0")
	assert.NoError(t, file.WriteFile(ctx, src0Path, []byte("a")))
	assert.NoError(t, file.WriteFile(ctx, src1Path, []byte("b")))
	assert.NoError(t, file.WriteFile(ctx, src2Path, []byte("c")))

	doExpand := func(str string) string {
		matches := expandGlob(ctx, tmpDir+"/"+str)
		for i := range matches {
			matches[i] = matches[i][len(tmpDir)+1:] // remove the tmpDir part.
		}
		return strings.Join(matches, ",")
	}

	assert.Equal(t, "abc/def/tmp0", doExpand("abc/*/tmp0"))
	assert.Equal(t, "xxx/yyy", doExpand("xxx/yyy"))
	assert.Equal(t, "xxx/*", doExpand("xxx/*"))
	assert.Equal(t, "abc/def/tmp0", doExpand("a*/*/tmp0"))
	assert.Equal(t, "abd/efg/hij/tmp1", doExpand("abd/**/tmp*"))
	assert.Equal(t, "abc/def/tmp0,abd/efg/hij/tmp1", doExpand("a*/**/tmp*"))
	assert.Equal(t, "abc/def/tmp0,abd/efg/hij/tmp1,tmp0", doExpand("**"))
}
