// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package http defines profile providers for local HTTP servers.
// It is imported for its side effects.
package http

import (
	"net/http"

	"github.com/grailbio/base/config"
	"github.com/grailbio/base/log"
)

func init() {
	config.Register("http", func(constr *config.Constructor) {
		addr := constr.String("addr", ":3333", "the address used for serving http")
		constr.Doc = "configure a local HTTP server, using the default http muxer"
		constr.New = func() (interface{}, error) {
			go func() {
				log.Print("http: serve ", *addr)
				err := http.ListenAndServe(*addr, nil)
				log.Error.Print("http: serve ", *addr, ": ", err)
			}()
			return nil, nil
		}
	})
}
