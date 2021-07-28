package grail

import (
	"github.com/grailbio/base/log"
	"v.io/x/lib/vlog"
)

// VlogOutputter implements base/log.Outputter backed by vlog.
type VlogOutputter struct{}

func (VlogOutputter) Level() log.Level {
	if vlog.V(1) {
		return log.Debug
	} else {
		return log.Info
	}
}

func (VlogOutputter) Output(calldepth int, level log.Level, s string) error {
	// Notice that we do not add 1 to the call depth. In vlog, 0 depth means
	// that the caller's file/line will be used. This is different from the log
	// and github.com/grailbio/base/log packages, where that's the behavior you
	// get with depth 1.
	switch level {
	case log.Off:
	case log.Error:
		vlog.ErrorDepth(calldepth, s)
	case log.Info:
		vlog.InfoDepth(calldepth, s)
	default:
		vlog.VI(vlog.Level(level)).InfoDepth(calldepth, s)
	}
	return nil
}
