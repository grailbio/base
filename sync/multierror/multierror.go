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

// MultiError is a mechanism for capturing errors from parallel
// go-routines. Usage:
//
//      errs := NewMultiError(3)
//      do := func(f foo) error {...}
//      for foo in range foos {
//          go errs.capture(do(foo))
//      }
//      // Wait for completion
//
// Will gather all errors returned in a MultiError, which in turn will
// behave as a normal error.
type MultiError struct {
	mu sync.Mutex

	errs  []error
	count int64
}

// NewMultiError creates a new MultiError struct.
func NewMultiError(max int) *MultiError {
	return &MultiError{errs: make([]error, 0, max), mu: sync.Mutex{}}
}

func (me *MultiError) add(err error) {
	if len(me.errs) == cap(me.errs) {
		me.count++
		return
	}

	me.errs = append(me.errs, err)
}

// Add captures an error from a go-routine and adds it to the MultiError.
func (me *MultiError) Add(err error) {
	if err == nil || me == nil {
		return
	}

	me.mu.Lock()
	defer me.mu.Unlock()

	multi, ok := err.(*MultiError)
	if ok {
		// Aggregate if it is a multierror.
		for _, e := range multi.errs {
			me.add(e)
		}
		me.count += multi.count
		return
	}

	me.add(err)
}

// Error returns a string version of the MultiError. This implements the error
// interface.
func (me *MultiError) Error() string {
	if me == nil {
		return ""
	}

	me.mu.Lock()
	defer me.mu.Unlock()

	if len(me.errs) == 0 {
		return ""
	}

	if len(me.errs) == 1 {
		return me.errs[0].Error()
	}

	s := make([]string, len(me.errs))
	for i, e := range me.errs {
		s[i] = e.Error()
	}
	errs := strings.Join(s, "\n")

	if me.count == 0 {
		return fmt.Sprintf("[%s]", errs)
	}

	return fmt.Sprintf("[%s] [plus %d other error(s)]", errs, me.count)
}

// Err returns an non-nil error representing all reported errors. Err
// returns nil when no errors where reported.
func (me *MultiError) Err() error {
	if me == nil {
		return nil
	}
	me.mu.Lock()
	defer me.mu.Unlock()
	switch len(me.errs) {
	case 0:
		return nil
	case 1:
		return me.errs[0]
	default:
		err := &multiError{
			errs:  make([]error, len(me.errs)),
			count: me.count,
		}
		for i := range err.errs {
			err.errs[i] = me.errs[i]
		}
		return err
	}
}
