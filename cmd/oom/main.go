// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/grailbio/base/stress/oom"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("oom: ")
	size := flag.Int("size", 0, "amount of memory to allocate; automatically determined if zero")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: oom [-size N]

OOM attempts to OOM the system by allocating up
N bytes of memory. If size is not specified, oom
automatically determines how much memory to allocate.
`)
		os.Exit(2)
	}
	flag.Parse()
	if *size != 0 {
		oom.Do(*size)
	}
	oom.Try()
}
