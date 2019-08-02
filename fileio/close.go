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
func CloseAndReport(f io.Closer, err *error) {
	err2 := f.Close()
	if err2 == nil {
		return
	}
	if *err != nil {
		var message string
		if namer, ok := f.(named); ok {
			message = fmt.Sprintf("second error on Close %s: %v", namer.Name(), err2)
		} else {
			message = fmt.Sprintf("second error on Close: %v", err2)
		}
		*err = errors.E(*err, message)
		return
	}
	*err = err2
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
