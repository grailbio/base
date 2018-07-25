// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package data

import "testing"

func TestSizeString(t *testing.T) {
	for _, c := range []struct {
		Size
		expect string
	}{
		{10 * B, "10B"},
		{727 * B, "727B"},
		{KiB, "1.0KiB"},
		{4*KiB + 100*B, "4.1KiB"},
		{73*KiB + 900*B, "73.9KiB"},
		{2*MiB + 800*KiB, "2.8MiB"},
		{TiB, "1.0TiB"},
		{2*EiB + 100000*TiB, "2.1EiB"},
	} {
		if got, want := c.Size.String(), c.expect; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := (-c.Size).String(), "-"+c.expect; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
