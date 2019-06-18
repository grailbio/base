package limitbuf

import "strings"

// Logger is like strings.Builder, but with maximum length.  If the caller tries
// to add data beyond the capacity, they will be dropped, and Logger.String()
// will append "(truncated)" at the end.
type Logger struct {
	maxLen       int
	truncated    bool
	addedTrailer bool
	b            strings.Builder
}

// NewLogger creates a new Logger object with the given capacity.
func NewLogger(maxLen int) *Logger {
	return &Logger{maxLen: maxLen}
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
	if n < len(data) {
		b.truncated = true
	}
	return len(data), nil
}

// String reports the data written so far. If the length of the data exceeds the
// buffer capacity, the prefix of the data, plus "(truncated)" will be reported.
func (b *Logger) String() string {
	if b.truncated {
		if !b.addedTrailer {
			b.b.WriteString("(truncated)")
			b.addedTrailer = true
		}
	}
	return b.b.String()
}
