package zstd_test

import (
	"flag"
	"io/ioutil"
	"os"
	"testing"

	"bytes"
	"io"

	"github.com/grailbio/base/compress/zstd"
	"github.com/grailbio/testutil/assert"
)

func TestCompress(t *testing.T) {
	z, err := zstd.CompressLevel(nil, []byte("hello"), -1)
	assert.NoError(t, err)
	assert.GT(t, len(z), 0)
	d, err := zstd.Decompress(nil, z)
	assert.NoError(t, err)
	assert.EQ(t, d, []byte("hello"))
}

func TestCompressScratch(t *testing.T) {
	z, err := zstd.CompressLevel(make([]byte, 3), []byte("hello"), -1)
	assert.NoError(t, err)
	assert.GT(t, len(z), 0)
	d, err := zstd.Decompress(make([]byte, 3), z)
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

var plaintextFlag = flag.String("plaintext", "", "plaintext file used in compression test")

func BenchmarkCompress(b *testing.B) {
	if *plaintextFlag == "" {
		b.Skip("--plaintext not set")
	}

	for i := 0; i < b.N; i++ {
		buf := bytes.Buffer{}
		w, err := zstd.NewWriter(&buf)
		assert.NoError(b, err)
		r, err := os.Open(*plaintextFlag)
		assert.NoError(b, err)
		_, err = io.Copy(w, r)
		assert.NoError(b, err)
		assert.NoError(b, w.Close())
		assert.NoError(b, r.Close())
	}
}

func BenchmarkUncompress(b *testing.B) {
	if *plaintextFlag == "" {
		b.Skip("--plaintext not set")
	}

	b.StopTimer()
	buf := bytes.Buffer{}
	w, err := zstd.NewWriter(&buf)
	assert.NoError(b, err)
	r, err := os.Open(*plaintextFlag)
	assert.NoError(b, err)
	_, err = io.Copy(w, r)
	assert.NoError(b, err)
	assert.NoError(b, w.Close())
	assert.NoError(b, r.Close())
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		zr, err := zstd.NewReader(bytes.NewReader(buf.Bytes()))
		assert.NoError(b, err)

		w := bytes.Buffer{}
		_, err = io.Copy(&w, zr)
		assert.NoError(b, err)
		assert.NoError(b, zr.Close())
	}
}
