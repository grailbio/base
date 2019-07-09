package fileio

import (
	"fmt"
	"io"

	"github.com/grailbio/base/errors"
)

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
func CloseAndReport(f io.Closer, err *error) {
	err2 := f.Close()
	if err2 == nil {
		return
	}
	if *err != nil {
		*err = errors.E(*err, fmt.Sprintf("second error in Close: %v", err2))
		return
	}
	*err = err2
}

// CloseOrPanic is a defer-able function that calls f.Close and panics on error.
//
// Example:
//   f, err := os.Open(filename)
//   if err != nil { panic(err) }
//   defer fileio.CloseOrPanic(f)
//   ...
func CloseOrPanic(f io.Closer) {
	if err := f.Close(); err != nil {
		panic(err)
	}
}
