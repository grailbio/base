// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package cmdutil provides utility routines for implementing command line
// tools.
package cmdutil

import (
	"fmt"
	"os"
	"strings"

	"v.io/x/lib/vlog"
)

// Fatalf mirrors log.Fatalf with no prefix and no timestamp.
func Fatalf(format string, args ...interface{}) {
	m := fmt.Sprintf(format, args...)
	fmt.Fprint(os.Stderr, strings.TrimSuffix(m, "\n")+"\n")
	vlog.FlushLog()
	os.Exit(1)
}

// Fatal mirrors log.Fatal with no prefix and no timestamp.
func Fatal(args ...interface{}) {
	m := fmt.Sprint(args...)
	fmt.Fprint(os.Stderr, strings.TrimSuffix(m, "\n")+"\n")
	vlog.FlushLog()
	os.Exit(1)
}
