// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordioutil

import (
	"fmt"

	"github.com/grailbio/base/fileio"
	"github.com/grailbio/base/recordio"
	"github.com/grailbio/base/recordio/recordioflate"
)

// ScannerV2OptsFromName returns the options to use for the supplied pathname
// based on its suffix.
func ScannerOptsFromName(path string) (opts recordio.ScannerOpts, err error) {
	compressed := false
	switch fileio.DetermineType(path) {
	case fileio.GrailRIOPackedCompressed:
		compressed = true
	case fileio.GrailRIOPackedEncrypted, fileio.GrailRIOPackedCompressedAndEncrypted:
		return opts, fmt.Errorf("%s: decrypting v1 recordio not supported", path)
	}
	switch {
	case compressed:
		opts.LegacyTransform = recordioflate.FlateUncompress
	}
	return
}
