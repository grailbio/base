package multierror

import (
	"errors"
	"testing"
)

func TestMultiError(t *testing.T) {
	me2a := NewMultiError(2)
	me2a.Add(errors.New("a"))
	me1ab := NewMultiError(1)
	me1ab.Add(errors.New("a"))
	me1ab.Add(errors.New("b"))

	for _, test := range []struct {
		errs     []error
		expected error
	}{
		{
			[]error{},
			nil,
		},
		{
			[]error{errors.New("FAIL")},
			errors.New("FAIL"),
		},
		{
			[]error{errors.New("1"), errors.New("2"), errors.New("3")},
			errors.New(`[1
2] [plus 1 other error(s)]`),
		},
		{
			[]error{errors.New("1"), me2a},
			errors.New(`[1
a]`),
		},
		{
			[]error{errors.New("1"), me1ab},
			errors.New(`[1
a] [plus 1 other error(s)]`),
		},
	} {
		errs := NewMultiError(2)

		for _, e := range test.errs {
			errs.Add(e)
		}

		got := errs.Err()

		if test.expected == nil && got == nil {
			continue
		}

		if test.expected == nil || got == nil {
			t.Fatalf("error mismatch: %v vs %v", test.expected, got)
		}

		if test.expected.Error() != got.Error() {
			t.Fatalf("error mismatch: %q vs %q", test.expected, got)
		}
	}
}
