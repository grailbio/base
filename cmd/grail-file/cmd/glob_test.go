package cmd

import (
	"context"
	"strings"
	"testing"

	"github.com/grailbio/base/file"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/assert"
)

func TestParseGlob(t *testing.T) {
	doParse := func(str string) string {
		prefix, hasGlob := parseGlob(str)
		if !hasGlob {
			return "none"
		}
		return prefix
	}
	assert.EQ(t, "none", doParse("s3://a/b/c"))
	assert.EQ(t, "none", doParse("s3://a/b\\*/c"))
	assert.EQ(t, "s3://a/", doParse("s3://a/b*/c"))
	assert.EQ(t, "s3://a/b/", doParse("s3://a/b/*"))
	assert.EQ(t, "s3://a/", doParse("s3://a/b?"))
	assert.EQ(t, "s3://a/", doParse("s3://a/**/b"))
	assert.EQ(t, "", doParse("**"))
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

	assert.EQ(t, "abc/def/tmp0", doExpand("abc/*/tmp0"))
	assert.EQ(t, "xxx/yyy", doExpand("xxx/yyy"))
	assert.EQ(t, "xxx/*", doExpand("xxx/*"))
	assert.EQ(t, "abc/def/tmp0", doExpand("a*/*/tmp0"))
	assert.EQ(t, "abd/efg/hij/tmp1", doExpand("abd/**/tmp*"))
	assert.EQ(t, "abc/def/tmp0,abd/efg/hij/tmp1", doExpand("a*/**/tmp*"))
	assert.EQ(t, "abc/def/tmp0,abd/efg/hij/tmp1,tmp0", doExpand("**"))
}
