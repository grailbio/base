package morebufio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/grailbio/base/ioctx"
	"github.com/grailbio/testutil/assert"
)

func TestReadSeeker(t *testing.T) {
	ctx := context.Background()
	const file = "0123456789"
	t.Run("read_zero", func(t *testing.T) {
		r := NewReadSeekerSize(ioctx.FromStdReadSeeker(bytes.NewReader([]byte(file))), 4)
		var b []byte
		n, err := r.Read(ctx, b)
		assert.NoError(t, err)
		assert.EQ(t, n, 0)
	})
	t.Run("read", func(t *testing.T) {
		r := NewReadSeekerSize(ioctx.FromStdReadSeeker(bytes.NewReader([]byte(file))), 4)
		b := make([]byte, 4)

		n, err := r.Read(ctx, b)
		assert.NoError(t, err)
		assert.GE(t, n, 0)
		assert.LE(t, n, len(b))
		assert.EQ(t, b[:n], []byte(file[:n]))
		remaining := file[n:]

		n, err = r.Read(ctx, b)
		assert.NoError(t, err)
		assert.GE(t, n, 0)
		assert.LE(t, n, len(b))
		assert.EQ(t, b[:n], []byte(remaining[:n]))
	})
	t.Run("seek", func(t *testing.T) {
		r := NewReadSeekerSize(ioctx.FromStdReadSeeker(bytes.NewReader([]byte(file))), 4)
		b := make([]byte, 4)

		n, err := io.ReadFull(ioctx.ToStdReadSeeker(ctx, r), b)
		assert.NoError(t, err)
		assert.EQ(t, n, len(b))
		assert.EQ(t, b, []byte(file[:4]))

		n64, err := r.Seek(ctx, -2, io.SeekCurrent)
		assert.NoError(t, err)
		assert.EQ(t, int(n64), 2)

		n, err = io.ReadFull(ioctx.ToStdReadSeeker(ctx, r), b)
		assert.NoError(t, err)
		assert.EQ(t, n, len(b))
		assert.EQ(t, b, []byte(file[2:6]))
	})
	t.Run("regression_early_eof", func(t *testing.T) {
		// Regression test for an issue discovered during NewReaderAt development.
		// We construct a read seeker that returns EOF after filling the internal buffer.
		// In this case we get that behavior from the string reader's ReadAt method, adapted
		// into a seeker. This exposed a bug where readSeeker returned the EOF from filling its
		// internal buffer even if the client hadn't read that far yet.
		rawRS := &readerAtSeeker{r: ioctx.FromStdReaderAt(strings.NewReader(file))}
		r := NewReadSeekerSize(rawRS, len(file)+1)
		b := make([]byte, 4)

		n, err := r.Read(ctx, b)
		assert.NoError(t, err)
		assert.EQ(t, n, len(b))
		assert.EQ(t, b, []byte(file[:4]))
	})
}

func TestReadSeekerRandom(t *testing.T) {
	const (
		fileSize = 10000
		testOps  = 100000
	)
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(1))
	file := func() string {
		b := make([]byte, fileSize)
		_, _ = rnd.Read(b)
		return string(b)
	}()
	for _, bufSize := range []int{1, 16, 1024, fileSize * 2} {
		t.Run(fmt.Sprint(bufSize), func(t *testing.T) {
			var (
				gold, test ioctx.ReadSeeker
				pos        int
			)
			reinit := func() {
				pos = 0
				gold = ioctx.FromStdReadSeeker(bytes.NewReader([]byte(file)))
				testBase := bytes.NewReader([]byte(file))
				if rnd.Intn(2) == 1 {
					// Exercise initializing in the middle of a file.
					pos = rnd.Intn(fileSize)
					nGold, err := gold.Seek(ctx, int64(pos), io.SeekStart)
					assert.NoError(t, err)
					assert.EQ(t, nGold, int64(pos))
					nTest, err := testBase.Seek(int64(pos), io.SeekStart)
					assert.NoError(t, err)
					assert.EQ(t, nTest, int64(pos))
				}
				test = NewReadSeekerSize(ioctx.FromStdReadSeeker(testBase), bufSize)
			}
			reinit()
			ops := []func(){
				reinit,
				func() { // read
					n := len(file) - pos
					if n > 0 {
						n = rnd.Intn(n)
					}
					bGold := make([]byte, n)
					bTest := make([]byte, len(bGold))

					nGold, errGold := io.ReadFull(ioctx.ToStdReadSeeker(ctx, gold), bGold)
					nTest, errTest := io.ReadFull(ioctx.ToStdReadSeeker(ctx, test), bTest)
					pos += nGold

					assert.EQ(t, nTest, nGold)
					assert.NoError(t, errGold)
					assert.NoError(t, errTest)
				},
				func() { // seek current
					off := rnd.Intn(len(file)) - pos

					nGold, errGold := gold.Seek(ctx, int64(off), io.SeekCurrent)
					nTest, errTest := test.Seek(ctx, int64(off), io.SeekCurrent)
					pos = int(nGold)

					assert.EQ(t, nTest, nGold)
					assert.NoError(t, errGold)
					assert.NoError(t, errTest)
				},
				func() { // seek start
					off := rnd.Intn(len(file))

					nGold, errGold := gold.Seek(ctx, int64(off), io.SeekStart)
					nTest, errTest := test.Seek(ctx, int64(off), io.SeekStart)
					pos = int(nGold)

					assert.EQ(t, nTest, nGold)
					assert.NoError(t, errGold)
					assert.NoError(t, errTest)
				},
				func() { // seek end
					off := -rnd.Intn(len(file))

					nGold, errGold := gold.Seek(ctx, int64(off), io.SeekEnd)
					nTest, errTest := test.Seek(ctx, int64(off), io.SeekEnd)
					pos = int(nGold)

					assert.EQ(t, nTest, nGold)
					assert.NoError(t, errGold)
					assert.NoError(t, errTest)
				},
			}
			for i := 0; i < testOps; i++ {
				ops[rnd.Intn(len(ops))]()
			}
		})
	}
}
