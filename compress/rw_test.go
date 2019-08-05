package compress_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/grailbio/base/compress"
	"github.com/grailbio/testutil/assert"
	"github.com/klauspost/compress/zstd"
)

func testReader(t *testing.T, plaintext string, comp func(t *testing.T, in []byte) []byte) {
	compressed := comp(t, []byte(plaintext))
	cr := bytes.NewReader(compressed)
	r, n := compress.NewReader(cr)
	assert.True(t, n)
	assert.NotNil(t, r)
	got := bytes.Buffer{}
	_, err := io.Copy(&got, r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.EQ(t, got.String(), plaintext)
}

// Generate a random ASCII text.
func randomText(buf *strings.Builder, r *rand.Rand, n int) {
	for i := 0; i < n; i++ {
		buf.WriteByte(byte(r.Intn(96) + 32))
	}
}

func gzipCompress(t *testing.T, in []byte) []byte {
	buf := bytes.Buffer{}
	w := gzip.NewWriter(&buf)
	_, err := io.Copy(w, bytes.NewReader(in))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
	return buf.Bytes()
}

func bzip2Compress(t *testing.T, in []byte) []byte {
	temp, err := ioutil.TempFile("", "test")
	assert.NoError(t, err)
	_, err = temp.Write(in)
	assert.NoError(t, err)
	assert.NoError(t, temp.Close())
	cmd := exec.Command("bzip2", temp.Name())
	assert.NoError(t, cmd.Run())

	compressed, err := ioutil.ReadFile(temp.Name() + ".bz2")
	assert.NoError(t, err)
	assert.NoError(t, os.Remove(temp.Name()+".bz2"))
	return compressed
}

func zstdCompress(t *testing.T, in []byte) []byte {
	buf := bytes.Buffer{}
	w, err := zstd.NewWriter(&buf)
	assert.NoError(t, err)
	_, err = io.Copy(w, bytes.NewReader(in))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
	return buf.Bytes()
}

type compressor struct {
	fn  func(t *testing.T, in []byte) []byte
	ext string
}

var compressors = []compressor{
	{zstdCompress, "zst"},
	{gzipCompress, "gz"},
	{bzip2Compress, "bz2"},
}

func TestReaderSmall(t *testing.T) {
	for _, c := range compressors {
		t.Run(c.ext, func(t *testing.T) {
			testReader(t, "", c.fn)
			testReader(t, "hello", c.fn)
		})
		n := 1
		for i := 1; i < 25; i++ {
			t.Run(fmt.Sprint("format=", c.ext, ",n=", n), func(t *testing.T) {
				r := rand.New(rand.NewSource(int64(i)))
				n = (n + 1) * 3 / 2
				buf := strings.Builder{}
				randomText(&buf, r, n)
				testReader(t, buf.String(), c.fn)
			})
		}
	}
}

func TestGzipReaderUncompressed(t *testing.T) {
	data := make([]byte, 128<<10+1)
	got := bytes.Buffer{}

	runTest := func(t *testing.T, n int) {
		for i := range data[:n] {
			// gzip/bzip2 header contains at least one char > 128, so the plaintext should
			// never be conflated with a gzip header.
			data[i] = byte(n + i%128)
		}
		cr := bytes.NewReader(data[:n])
		r, compressed := compress.NewReader(cr)
		assert.False(t, compressed)
		got.Reset()
		nRead, err := io.Copy(&got, r)
		assert.NoError(t, err)
		assert.EQ(t, int(nRead), n)
		assert.NoError(t, r.Close())
		assert.EQ(t, got.Bytes(), data[:n])
	}

	dataSize := 1
	for dataSize <= len(data) {
		n := dataSize
		t.Run(fmt.Sprint(n), func(t *testing.T) { runTest(t, n) })
		t.Run(fmt.Sprint(n-1), func(t *testing.T) { runTest(t, n-1) })
		t.Run(fmt.Sprint(n+1), func(t *testing.T) { runTest(t, n+1) })
		dataSize *= 2
	}
}

func TestReaderWriterPath(t *testing.T) {
	for _, c := range compressors {
		t.Run(c.ext, func(t *testing.T) {
			if c.ext == "bz2" { // bz2 compression not yet supported
				t.Skip("bz2")
			}
			buf := bytes.Buffer{}
			w, compressed := compress.NewWriterPath(&buf, "foo."+c.ext)
			assert.True(t, compressed)
			_, err := io.WriteString(w, "hello")
			assert.NoError(t, w.Close())
			assert.NoError(t, err)

			r, compressed := compress.NewReaderPath(&buf, "foo."+c.ext)
			assert.True(t, compressed)
			data, err := ioutil.ReadAll(r)
			assert.NoError(t, err)
			assert.EQ(t, string(data), "hello")
			assert.NoError(t, r.Close())
		})
	}
}

// NewReaderPath and NewWriterPath for non-compressed extensions.
func TestReaderWriterPathNop(t *testing.T) {
	buf := bytes.Buffer{}
	w, compressed := compress.NewWriterPath(&buf, "foo.txt")
	assert.False(t, compressed)
	_, err := io.WriteString(w, "hello")
	assert.NoError(t, w.Close())
	assert.NoError(t, err)

	r, compressed := compress.NewReaderPath(&buf, "foo.txt")
	assert.False(t, compressed)
	data, err := ioutil.ReadAll(r)
	assert.NoError(t, err)
	assert.EQ(t, string(data), "hello")
	assert.NoError(t, r.Close())
}
