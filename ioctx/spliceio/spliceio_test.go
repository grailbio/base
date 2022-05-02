package spliceio_test

import (
	"github.com/grailbio/base/file/fsnodefuse"
	"github.com/grailbio/base/ioctx/spliceio"
)

// Check this here to avoid circular package dependency with fsnodefuse.
var _ fsnodefuse.Writable = (*spliceio.OSFile)(nil)
