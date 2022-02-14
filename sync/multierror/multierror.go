package multierror

import (
	"fmt"
	"strings"
	"sync"
)

type multiError struct {
	errs    []error
	dropped int
}

// Error returns a string describing the multiple errors represented by e.
func (e multiError) Error() string {
	switch len(e.errs) {
	case 0, 1:
		panic("invalid multiError")
	default:
		var b strings.Builder
		b.WriteString("[")
		for i, err := range e.errs {
			if i > 0 {
				b.WriteString("\n")
			}
			b.WriteString(err.Error())
		}
		b.WriteString("]")
		if e.dropped > 0 {
			fmt.Fprintf(&b, " [plus %d other error(s)]", e.dropped)
		}
		return b.String()
	}
}

// Builder captures errors from parallel goroutines.
//
// Example usage:
// 	var (
// 		errs = multierror.NewBuilder(3)
// 		wg   sync.WaitGroup
// 	)
// 	for _, foo := range foos {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			errs.Add(someWork(foo))
// 		}()
// 	}
// 	wg.Wait()
// 	if err := errs.Err(); err != nil {
// 		// handle err
// 	}
type Builder struct {
	mu      sync.Mutex // mu guards all fields
	errs    []error
	dropped int
}

func NewBuilder(max int) *Builder {
	return &Builder{errs: make([]error, 0, max)}
}

// Add adds an error to b. b must be non-nil.
func (b *Builder) Add(err error) {
	if err == nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.errs) == cap(b.errs) {
		b.dropped++
		return
	}
	b.errs = append(b.errs, err)
}

// Err returns an error combining all the errors that were already Add-ed. Otherwise returns nil.
// b may be nil.
func (b *Builder) Err() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch len(b.errs) {
	case 0:
		return nil
	case 1:
		// TODO: This silently ignores b.dropped which is bad because it may be non-zero.
		// Maybe we should make multiError{*, 1} legal. Or, maybe forbid max < 2.
		return b.errs[0]
	default:
		return multiError{
			// Sharing b.errs is ok because multiError doesn't mutate or append and Builder
			// only appends.
			errs:    b.errs,
			dropped: b.dropped,
		}
	}
}
