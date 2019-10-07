// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package vcontext creates a singleton vanadium Context object.
package vcontext

import (
	"sync"

	"github.com/grailbio/base/backgroundcontext"
	"github.com/grailbio/base/shutdown"
	_ "github.com/grailbio/v23/factories/grail" // Needed to initialize v23
	v23 "v.io/v23"
	"v.io/v23/context"
	"v.io/x/ref/runtime/factories/library"
)

var (
	once = sync.Once{}
	ctx  *context.T
)

func init() {
	library.AllowMultipleInitializations = true
}

// Background returns the singleton Vanadium context for v23. It initializes v23
// on the first call.  GRAIL applications should always use this function to
// initialize and create a context instead of calling v23.Init() manually.
//
// Caution: this function is depended on by many services, specifically the
// production pipeline controller. Be extremely careful when changing it.
func Background() *context.T {
	once.Do(func() {
		var done v23.Shutdown
		ctx, done = v23.Init()
		shutdown.Register(shutdown.Func(done))
		backgroundcontext.Set(ctx)
	})
	return ctx
}
