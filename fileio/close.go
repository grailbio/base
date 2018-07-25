package fileio

import (
	"fmt"
	"io"

	"github.com/grailbio/base/errors"
)

type named interface {
	// Name returns the path name.
	Name() string
}

// CloseAndReport returns a defer-able helper that calls f.Close and reports errors, if any,
// to *err. Pass your function's named return error. Example usage:
//
//   func processFile(filename string) (_ int, err error) {
//     f, err := os.Open(filename)
//     if err != nil { ... }
//     defer fileio.CloseAndReport(f, &err)
//     ...
//   }
//
// If your function returns with an error, any f.Close error will be chained appropriately.
//
// Deprecated: Use errors.CleanUp directly.
func CloseAndReport(f io.Closer, err *error) {
	errors.CleanUp(f.Close, err)
}

// MustClose is a defer-able function that calls f.Close and panics on error.
//
// Example:
//   f, err := os.Open(filename)
//   if err != nil { panic(err) }
//   defer fileio.MustClose(f)
//   ...
func MustClose(f io.Closer) {
	if err := f.Close(); err != nil {
		if n, ok := f.(named); ok {
			panic(fmt.Sprintf("close %s: %v", n.Name(), err))
		}
		panic(err)
	}
}
