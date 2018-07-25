package errors

import (
	"context"
	"fmt"
)

// CleanUp is defer-able syntactic sugar that calls f and reports an error, if any,
// to *err. Pass the caller's named return error. Example usage:
//
//   func processFile(filename string) (_ int, err error) {
//     f, err := os.Open(filename)
//     if err != nil { ... }
//     defer errors.CleanUp(f.Close, &err)
//     ...
//   }
//
// If the caller returns with its own error, any error from cleanUp will be chained.
func CleanUp(cleanUp func() error, dst *error) {
	addErr(cleanUp(), dst)
}

// CleanUpCtx is CleanUp for a context-ful cleanUp.
func CleanUpCtx(ctx context.Context, cleanUp func(context.Context) error, dst *error) {
	addErr(cleanUp(ctx), dst)
}

func addErr(err2 error, dst *error) {
	if err2 == nil {
		return
	}
	if *dst == nil {
		*dst = err2
		return
	}
	// Note: We don't chain err2 as *dst's cause because *dst may already have a meaningful cause.
	// Also, even if *dst didn't, err2 may be something entirely different, and suggesting it's
	// the cause could be misleading.
	// TODO: Consider using a standardized multiple-errors representation like sync/multierror's.
	*dst = E(*dst, fmt.Sprintf("second error in Close: %v", err2))
}
