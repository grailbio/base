// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordioutil

import (
	"github.com/grailbio/base/fileio"
	"github.com/klauspost/compress/flate"
)

// SuffixFromWriterOpts returns the file name suffix to use for the specified
// Writer options.
func SuffixFromWriterOpts(opts WriterOpts) string {
	filetype := fileio.Other
	compressed := false
	if opts.FlateLevel != flate.NoCompression {
		compressed = true
	}
	switch {
	case compressed:
		filetype = fileio.GrailRIOPackedCompressed
	default:
		filetype = fileio.GrailRIOPacked
	}
	return fileio.FileSuffix(filetype)
}
