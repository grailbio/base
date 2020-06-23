package morebufio

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/grailbio/testutil/assert"
)

func TestReadSeeker(t *testing.T) {
	const file = "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"
	t.Run("read_zero", func(t *testing.T) {
		r := NewReadSeekerSize(bytes.NewReader([]byte(file)), 4)
		var b []byte
		n, err := r.Read(b)
		assert.NoError(t, err)
		assert.EQ(t, n, 0)
	})
	t.Run("read", func(t *testing.T) {
		r := NewReadSeekerSize(bytes.NewReader([]byte(file)), 4)
		b := make([]byte, 4)

		n, err := r.Read(b)
		assert.NoError(t, err)
		assert.GE(t, n, 0)
		assert.LE(t, n, len(b))
		assert.EQ(t, b[:n], []byte(file[:n]))
		remaining := file[n:]

		n, err = r.Read(b)
		assert.NoError(t, err)
		assert.GE(t, n, 0)
		assert.LE(t, n, len(b))
		assert.EQ(t, b[:n], []byte(remaining[:n]))
	})
	t.Run("seek", func(t *testing.T) {
		r := NewReadSeekerSize(bytes.NewReader([]byte(file)), 4)
		b := make([]byte, 4)

		n, err := io.ReadFull(r, b)
		assert.NoError(t, err)
		assert.EQ(t, n, len(b))
		assert.EQ(t, b, []byte(file[:4]))

		n64, err := r.Seek(-2, io.SeekCurrent)
		assert.NoError(t, err)
		assert.EQ(t, int(n64), 2)

		n, err = io.ReadFull(r, b)
		assert.NoError(t, err)
		assert.EQ(t, n, len(b))
		assert.EQ(t, b, []byte(file[2:6]))
	})
}

func TestReadSeekerRandom(t *testing.T) {
	const (
		fileSize = 10000
		testOps  = 100000
	)
	rnd := rand.New(rand.NewSource(1))
	file := func() string {
		b := make([]byte, fileSize)
		_, _ = rnd.Read(b)
		return string(b)
	}()
	for _, bufSize := range []int{1, 16, 1024, fileSize * 2} {
		t.Run(fmt.Sprint(bufSize), func(t *testing.T) {
			var (
				gold, test io.ReadSeeker
				pos        int
			)
			reinit := func() {
				pos = 0
				gold = bytes.NewReader([]byte(file))
				testBase := bytes.NewReader([]byte(file))
				if rnd.Intn(2) == 1 {
					// Exercise initializing in the middle of a file.
					pos = rnd.Intn(fileSize)
					nGold, err := gold.Seek(int64(pos), io.SeekStart)
					assert.NoError(t, err)
					assert.EQ(t, nGold, int64(pos))
					nTest, err := testBase.Seek(int64(pos), io.SeekStart)
					assert.NoError(t, err)
					assert.EQ(t, nTest, int64(pos))
				}
				test = NewReadSeekerSize(testBase, bufSize)
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

					nGold, errGold := io.ReadFull(gold, bGold)
					nTest, errTest := io.ReadFull(test, bTest)
					pos += nGold

					assert.EQ(t, nTest, nGold)
					assert.NoError(t, errGold)
					assert.NoError(t, errTest)
				},
				func() { // seek current
					off := rnd.Intn(len(file)) - pos

					nGold, errGold := gold.Seek(int64(off), io.SeekCurrent)
					nTest, errTest := test.Seek(int64(off), io.SeekCurrent)
					pos = int(nGold)

					assert.EQ(t, nTest, nGold)
					assert.NoError(t, errGold)
					assert.NoError(t, errTest)
				},
				func() { // seek start
					off := rnd.Intn(len(file))

					nGold, errGold := gold.Seek(int64(off), io.SeekStart)
					nTest, errTest := test.Seek(int64(off), io.SeekStart)
					pos = int(nGold)

					assert.EQ(t, nTest, nGold)
					assert.NoError(t, errGold)
					assert.NoError(t, errTest)
				},
				func() { // seek end
					off := -rnd.Intn(len(file))

					nGold, errGold := gold.Seek(int64(off), io.SeekEnd)
					nTest, errTest := test.Seek(int64(off), io.SeekEnd)
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
