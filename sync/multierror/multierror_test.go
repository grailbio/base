package multierror

import (
	"errors"
	"testing"
)

func TestMultiError(t *testing.T) {
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
			[]error{errors.New("1"), NewMultiError(2).Add(errors.New("a"))},
			errors.New(`[1
a]`),
		},
		{
			[]error{errors.New("1"), NewMultiError(1).Add(errors.New("a")).Add(errors.New("b"))},
			errors.New(`[1
a] [plus 1 other error(s)]`),
		},
	} {
		errs := NewMultiError(2)

		for _, e := range test.errs {
			errs.Add(e)
		}

		got := errs.ErrorOrNil()

		if test.expected == nil && got == nil {
			continue
		}

		if test.expected == nil || got == nil {
			t.Fatalf("error mismatch: %v vs %v", test.expected, got)
		}

		if test.expected.Error() != got.Error() {
			t.Fatalf("error mismatch: %v vs %v", test.expected, got)
		}
	}
}
