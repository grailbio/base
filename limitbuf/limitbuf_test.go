package limitbuf_test

import (
	"bytes"
	"testing"

	"github.com/grailbio/base/limitbuf"
	"github.com/grailbio/base/log"
	"github.com/grailbio/testutil/expect"
)

func TestLogger(t *testing.T) {
	l := limitbuf.NewLogger(10)
	l.Write([]byte("blah"))
	expect.EQ(t, l.String(), "blah")
	l.Write([]byte("abcdefgh"))
	expect.EQ(t, l.String(), "blahabcdef(truncated 2 bytes)")
	expect.EQ(t, l.String(), "blahabcdef(truncated 2 bytes)")
}

func TestLoggerExtremeTruncation(t *testing.T) {
	oldOutputter := log.GetOutputter()
	t.Cleanup(func() { log.SetOutputter(oldOutputter) })
	var outputter testOutputter
	log.SetOutputter(&outputter)

	logger := limitbuf.NewLogger(2, limitbuf.LogIfTruncatingMaxMultiple(3))
	_, err := logger.Write([]byte("abcdefg"))
	expect.NoError(t, err)

	expect.EQ(t, logger.String(), "ab(truncated 5 bytes)")
	expect.HasSubstr(t, outputter.String(), "extreme truncation")
}

type testOutputter struct{ bytes.Buffer }

func (o *testOutputter) Level() log.Level {
	return log.Error
}
func (o *testOutputter) Output(_ int, _ log.Level, s string) error {
	_, err := o.Buffer.WriteString(s)
	return err
}
