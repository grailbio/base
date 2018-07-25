// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

//+build !unit

package backgroundcontext_test

import (
	"context"
	"testing"

	"github.com/grailbio/base/backgroundcontext"
	"github.com/grailbio/base/vcontext"
	v23 "v.io/v23"
	v23context "v.io/v23/context"
)

func TestWrap(t *testing.T) {
	// This sets the backgroundcontext.
	_ = vcontext.Background()

	ctx, cancel := context.WithCancel(context.Background())
	bgctx := backgroundcontext.Wrap(ctx)
	if v23.GetClient(v23context.FromGoContext(bgctx)) == nil {
		t.Fatal("no v23 client returned")
	}
	cancel()
	if got, want := bgctx.Err(), context.Canceled; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
