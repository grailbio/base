package s3bufpool

import (
	"sync"
)

var (
	BufBytes = 16 * 1024 * 1024
	pool     = sync.Pool{
		New: func() any {
			b := make([]byte, BufBytes)
			// Note: Return *[]byte, not []byte, so there's one heap allocation now to create the
			// interface value, rather than one per Put.
			return &b
		},
	}
)

func Get() *[]byte  { return pool.Get().(*[]byte) }
func Put(b *[]byte) { pool.Put(b) }

// SetBufSize modifies the buffer size. It's for testing only, and callers are responsible for
// making sure there's no race with Get or Put.
func SetBufSize(bytes int) {
	BufBytes = bytes
	pool = sync.Pool{New: pool.New} // Empty the pool.
}
