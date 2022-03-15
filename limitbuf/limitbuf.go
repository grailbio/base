package limitbuf

import (
	"fmt"
	"strings"

	"github.com/grailbio/base/log"
)

type (
	// Logger is like strings.Builder, but with maximum length.  If the caller tries
	// to add data beyond the capacity, they will be dropped, and Logger.String()
	// will append "(truncated)" at the end.
	//
	// TODO: Consider renaming to Builder or Buffer since this type's behavior
	// is analogous to those.
	Logger struct {
		maxLen int
		b      strings.Builder

		// seen counts the total number of bytes passed to Write.
		seen                       int64
		logIfTruncatingMaxMultiple float64
	}

	// LoggerOption is passed to NewLogger to configure a Logger.
	LoggerOption func(*Logger)
)

// LogIfTruncatingMaxMultiple controls informative logging about how much data
// passed to Write has been truncated.
//
// If zero, this logging is disabled. Otherwise, if the sum of len(data)
// passed to prior Write calls is greater than LogIfTruncatingMaxMultiple *
// maxLen (passed to NewLogger), a log message is written in the next call
// to String(). After logging, LogIfTruncatingMaxMultiple is set to zero
// to avoid repeating the same message.
//
// This can be a useful diagnostic for both CPU and memory usage if a huge
// amount of data is written and only a tiny fraction is used. For example,
// if a caller writes to the log with fmt.Fprint(logger, ...) they may
// not realize that fmt.Fprint* actually buffers the *entire* formatted
// string in memory first, then writes it to logger.
// TODO: Consider serving the fmt use case better for e.g. bigslice.
//
// Note that the log message is written to log.Error, not the Logger itself
// (it's not part of String's return).
func LogIfTruncatingMaxMultiple(m float64) LoggerOption {
	return func(l *Logger) { l.logIfTruncatingMaxMultiple = m }
}

// NewLogger creates a new Logger object with the given capacity.
func NewLogger(maxLen int, opts ...LoggerOption) *Logger {
	l := Logger{maxLen: maxLen}
	for _, opt := range opts {
		opt(&l)
	}
	return &l
}

// Write implements io.Writer interface.
func (b *Logger) Write(data []byte) (int, error) {
	n := b.maxLen - b.b.Len()
	if n > len(data) {
		n = len(data)
	}
	if n > 0 {
		b.b.Write(data[:n])
	}
	b.seen += int64(len(data))
	return len(data), nil
}

// String reports the data written so far. If the length of the data exceeds the
// buffer capacity, the prefix of the data, plus "(truncated)" will be reported.
func (b *Logger) String() string {
	if b.seen <= int64(b.maxLen) {
		return b.b.String()
	}
	// Truncated.
	if b.logIfTruncatingMaxMultiple > 0 &&
		b.seen > int64(float64(b.maxLen)*b.logIfTruncatingMaxMultiple) {
		b.logIfTruncatingMaxMultiple = 0
		log.Errorf("limitbuf: extreme truncation: %d -> %d bytes", b.seen, b.maxLen)
	}
	return b.b.String() + fmt.Sprintf("(truncated %d bytes)", b.seen-int64(b.maxLen))
}
