// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordioutil_test

import (
	"testing"

	"github.com/grailbio/base/recordio/recordioutil"
	"github.com/klauspost/compress/flate"
)

func TestSuffix(t *testing.T) {
	wropts := func(compress bool) recordioutil.WriterOpts {
		opts := recordioutil.WriterOpts{}
		if compress {
			opts.FlateLevel = flate.BestCompression
		}
		return opts
	}

	for i, tc := range []struct {
		opts   recordioutil.WriterOpts
		suffix string
	}{
		{wropts(false), ".grail-rpk"},
		{wropts(true), ".grail-rpk-gz"},
	} {
		if got, want := recordioutil.SuffixFromWriterOpts(tc.opts), tc.suffix; got != want {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}
	}
}
