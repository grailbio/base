package zstd_test

import (
	"io/ioutil"
	"testing"

	"bytes"
	"io"

	"github.com/grailbio/base/compress/zstd"
	"github.com/grailbio/testutil/assert"
)

func TestCompress(t *testing.T) {
	z, err := zstd.Compress(nil, []byte("hello"))
	assert.NoError(t, err)
	assert.GT(t, len(z), 0)
	d, err := zstd.Decompress(nil, z)
	assert.NoError(t, err)
	assert.EQ(t, d, []byte("hello"))
}

func TestReadWrite(t *testing.T) {
	buf := bytes.Buffer{}
	w, err := zstd.NewWriter(&buf)
	assert.NoError(t, err)
	_, err = io.WriteString(w, "hello2")
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	r, err := zstd.NewReader(&buf)
	assert.NoError(t, err)
	d, err := ioutil.ReadAll(r)
	assert.NoError(t, err)
	assert.EQ(t, d, []byte("hello2"))
}
