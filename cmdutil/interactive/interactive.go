// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package interactive is switching defaults for logging to not output
// anything to stderr.
//
// All user-facing programs should use this via an import:
//
//   import _ "github.com/grailbio/base/cmdutil/interactive"
package interactive

import (
	"flag"

	// For the flag.Lookup calls.
	_ "v.io/x/lib/vlog"
)

func init() {
	fl := flag.Lookup("alsologtostderr")
	fl.DefValue = "false"
	if err := fl.Value.Set(fl.DefValue); err != nil {
		panic(err)
	}

	fl = flag.Lookup("logtostderr")
	fl.DefValue = "false"
	if err := fl.Value.Set(fl.DefValue); err != nil {
		panic(err)
	}
}
