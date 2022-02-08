package multierror

import (
	"fmt"
	"strings"
	"sync"
)

type multiError struct {
	errs  []error
	count int64
}

// Err returns an non-nil error representing all reported errors. Err
// returns nil when no errors where reported.
func (e *multiError) Error() string {
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
		if e.count > 0 {
			fmt.Fprintf(&b, " [plus %d other error(s)]", e.count)
		}
		return b.String()
	}
}

// Builder is a mechanism for capturing errors from parallel
// go-routines. Usage:
//
//      errs := NewBuilder(3)
//      do := func(f foo) error {...}
//      for foo in range foos {
//          go errs.capture(do(foo))
//      }
//      // Wait for completion
//      if errs.Err() != nil { /* handle error */ }
type Builder struct {
	mu    sync.Mutex // mu guards all fields
	errs  []error
	count int64
}

func NewBuilder(max int) *Builder {
	return &Builder{errs: make([]error, 0, max)}
}

func (b *Builder) add(err error) {
	if len(b.errs) == cap(b.errs) {
		b.count++
		return
	}

	b.errs = append(b.errs, err)
}

// Add captures an error from a go-routine and adds it to the Builder.
func (b *Builder) Add(err error) {
	if err == nil || b == nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	multi, ok := err.(*Builder)
	if ok {
		// Aggregate if it is a Builder.
		for _, e := range multi.errs {
			b.add(e)
		}
		b.count += multi.count
		return
	}

	b.add(err)
}

// Error returns a string version of the Builder. This implements the error
// interface.
func (b *Builder) Error() string {
	if b == nil {
		return ""
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.errs) == 0 {
		return ""
	}

	if len(b.errs) == 1 {
		return b.errs[0].Error()
	}

	s := make([]string, len(b.errs))
	for i, e := range b.errs {
		s[i] = e.Error()
	}
	errs := strings.Join(s, "\n")

	if b.count == 0 {
		return fmt.Sprintf("[%s]", errs)
	}

	return fmt.Sprintf("[%s] [plus %d other error(s)]", errs, b.count)
}

// Err returns an non-nil error representing all reported errors. Err
// returns nil when no errors where reported.
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
		return b.errs[0]
	default:
		err := &multiError{
			errs:  make([]error, len(b.errs)),
			count: b.count,
		}
		for i := range err.errs {
			err.errs[i] = b.errs[i]
		}
		return err
	}
}
