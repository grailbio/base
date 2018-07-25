// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"

	_ "github.com/grailbio/base/cmdutil/interactive"
	"github.com/grailbio/base/grail"
	"v.io/x/lib/vlog"
)

func main() {
	shutdown := grail.Init()
	defer shutdown()

	fmt.Printf("%v\n", vlog.Log.LogDir())
	vlog.Infof("-----")
	for i, a := range flag.Args() {
		vlog.Infof("T: %d: %v", i, a)
		vlog.VI(2).Infof("V2: %d: %v", i, a)
	}
	vlog.FlushLog()
}
