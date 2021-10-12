package fsnodefuse

import (
	"runtime/debug"
	"syscall"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"github.com/hanwen/go-fuse/v2/fs"
)

// TODO: Dedupe with gfs.go.
func errToErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	log.Debug.Printf("error %v: stack=%s", err, string(debug.Stack()))
	switch {
	case err == nil:
		return 0
	case errors.Is(errors.Timeout, err):
		return syscall.ETIMEDOUT
	case errors.Is(errors.Canceled, err):
		return syscall.EINTR
	case errors.Is(errors.NotExist, err):
		return syscall.ENOENT
	case errors.Is(errors.Exists, err):
		return syscall.EEXIST
	case errors.Is(errors.NotAllowed, err):
		return syscall.EACCES
	case errors.Is(errors.Integrity, err):
		return syscall.EIO
	case errors.Is(errors.Invalid, err):
		return syscall.EINVAL
	case errors.Is(errors.Precondition, err), errors.Is(errors.Unavailable, err):
		return syscall.EAGAIN
	case errors.Is(errors.Net, err):
		return syscall.ENETUNREACH
	case errors.Is(errors.TooManyTries, err):
		log.Error.Print(err)
		return syscall.EINVAL
	}
	return fs.ToErrno(err)
}
