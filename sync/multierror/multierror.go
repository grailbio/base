package multierror

import (
	"fmt"
	"strings"
	"sync"
)

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
	errs  []error
	count int64
	mu    sync.Mutex
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

// ErrorOrNil returns nil if no errors were captured, itself otherwise.
func (me *MultiError) ErrorOrNil() error {
	if me == nil {
		return nil
	}

	me.mu.Lock()
	defer me.mu.Unlock()

	if len(me.errs) == 0 {
		return nil
	}

	return me
}
