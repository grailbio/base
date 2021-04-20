// +build cgo

package zstd_test

import (
	"bytes"
	"io/ioutil"
	"testing"

	cgozstd "github.com/DataDog/zstd"
	"github.com/grailbio/base/must"
	nocgozstd "github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
)

// TestAliasingCorruption reproduces data corruption.
// It fails at:
//   github.com/DataDog/zstd@v1.4.5-0.20200131134752-ae82924b5fc5
// and passes at the next commit:
//   github.com/DataDog/zstd@v1.4.5-0.20200131162126-ca147a0433e2
// suggesting that https://github.com/DataDog/zstd/pull/79 fixed the underlying problem, as
// reported previously in the linked issues.
func TestAliasingCorruption(t *testing.T) {
	t.Skip()
	const (
		want1 = "\x11\n\x00\x0e" + "01234567890123"
		want2 = "\x18\n\x00\x15" + "012345678901230123456"
		want  = want1 + want2
	)
	test := func(t *testing.T, modifyWrite1 func([]byte)) {
		var (
			write1 = []byte(want1)
			write2 = []byte(want2)
			buf    bytes.Buffer
		)
		wc := cgozstd.NewWriter(&buf)
		_, err := wc.Write(write1)
		must.Nil(err)

		modifyWrite1(write1) // We already wrote write1. This _should_ be a no-op.

		_, err = wc.Write(write2)
		must.Nil(err)
		must.Nil(wc.Close())

		rc := cgozstd.NewReader(&buf)
		got, err := ioutil.ReadAll(rc)
		must.Nil(err)
		must.Nil(rc.Close())
		assert.Equal(t, []byte(want), got)
	}
	t.Run("aliasing_unobserved", func(t *testing.T) {
		test(t, func(write1 []byte) {
			// Leave unchanged.
		})
	})
	t.Run("overwrite_write1", func(t *testing.T) {
		test(t, func(write1 []byte) {
			copy(write1, want2)
		})
	})
	t.Run("overwrite_just_1_byte", func(t *testing.T) {
		test(t, func(write1 []byte) {
			write1[3] = '\x15'
		})
	})

	// The error occurs when DataDog/zstd is used in writes (and klauspost's for read), but not
	// vice versa, suggesting the corruption occurs in the write path.
	t.Run("datadog_write_only", func(t *testing.T) {
		var (
			write1 = []byte(want1)
			write2 = []byte(want2)
			buf    bytes.Buffer
		)
		wc := cgozstd.NewWriter(&buf)
		_, err := wc.Write(write1)
		must.Nil(err)

		copy(write1, want2)

		_, err = wc.Write(write2)
		must.Nil(err)
		must.Nil(wc.Close())

		rc, err := nocgozstd.NewReader(&buf)
		must.Nil(err)
		got, err := ioutil.ReadAll(rc)
		must.Nil(err)
		rc.Close()
		assert.Equal(t, []byte(want), got)
	})
	t.Run("datadog_read_only", func(t *testing.T) {
		var (
			write1 = []byte(want1)
			write2 = []byte(want2)
			buf    bytes.Buffer
		)
		wc, err := nocgozstd.NewWriter(&buf)
		must.Nil(err)
		_, err = wc.Write(write1)
		must.Nil(err)

		copy(write1, want2)

		_, err = wc.Write(write2)
		must.Nil(err)
		must.Nil(wc.Close())

		rc := cgozstd.NewReader(&buf)
		got, err := ioutil.ReadAll(rc)
		must.Nil(err)
		must.Nil(rc.Close())
		assert.Equal(t, []byte(want), got)
	})
}
