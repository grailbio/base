// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"strings"
)

// Strings is a comma-separated list of string flag, like
// `-myflag=foo,bar`. Subsequent flags will replace the previous list, not
// append: `-myflag=foo,bar -myflag=baz` yields []string{'baz'}.
type FlagStrings []string

// String implements flag.Value.
func (is FlagStrings) String() string { return strings.Join(is, ",") }

// Set implements flag.Value.
func (is *FlagStrings) Set(s string) error {
	if s == "" {
		*is = nil
	} else {
		*is = strings.Split(s, ",")
	}
	return nil
}
