package fsnodefuse

import (
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"syscall"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"github.com/hanwen/go-fuse/v2/fs"
)

// numHandledPanics is the total number of panics handled by handlePanicErrno.
// It can be used in testing to verify whether we triggered panics when
// handling operations.  It must be accessed atomically, i.e. using atomic.*
// functions.
var numHandledPanics uint32

// handlePanicErrno is a last resort to prevent panics from reaching go-fuse and breaking the FUSE mount.
// All go-fuse-facing APIs that return Errno should defer it.
func handlePanicErrno(errno *syscall.Errno) {
	r := recover()
	if r == nil {
		return
	}
	atomic.AddUint32(&numHandledPanics, 1)
	*errno = errToErrno(makePanicErr(r))
}

// handlePanicErr is like handlePanicErrno but for APIs that don't return Errno.
func handlePanicErr(dst *error) {
	r := recover()
	if r == nil {
		return
	}
	*dst = makePanicErr(r)
}

func makePanicErr(recovered interface{}) error {
	if err, ok := recovered.(error); ok {
		return errors.E(err, fmt.Sprintf("recovered panic, stack:\n%v", string(debug.Stack())))
	}
	return errors.E(fmt.Sprintf("recovered panic: %v, stack:\n%v", recovered, string(debug.Stack())))
}

func errToErrno(err error) syscall.Errno {
	if err == nil {
		return fs.OK
	}
	e := errors.Recover(err)
	kind := e.Kind
	errno, ok := kind.Errno()
	if ok {
		log.Error.Printf("returning errno: %v for error: %v", errno, e)
		return errno
	}
	log.Error.Printf("error with no good errno match: kind: %v, err: %v", kind, err)
	return syscall.EIO
}
