// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build ignore

package main

import (
	"crypto/rand"
	"fmt"
	"strings"
)

func main() {
	buf := make([]byte, 8, 8)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err)
	}
	out := "[]byte{ "
	for _, v := range buf {
		out += fmt.Sprintf("0x%02x, ", v)
	}
	out = strings.TrimSuffix(out, ", ") + " }"
	fmt.Println(out)
}
