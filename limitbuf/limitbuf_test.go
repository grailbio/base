package limitbuf_test

import (
	"testing"

	"github.com/grailbio/base/limitbuf"
	"github.com/grailbio/testutil/expect"
)

func TestLogger(t *testing.T) {
	l := limitbuf.NewLogger(10)
	l.Write([]byte("blah"))
	expect.EQ(t, l.String(), "blah")
	l.Write([]byte("abcdefgh"))
	expect.EQ(t, l.String(), "blahabcdef(truncated)")
	expect.EQ(t, l.String(), "blahabcdef(truncated)")
}
