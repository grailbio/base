// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package status

import (
	"net/http"
)

type statusHandler struct{ *Status }

// Handler returns a HTTP handler that renders a simple plain-text status
// snapshot of s on each request.
func Handler(s *Status) http.Handler {
	return statusHandler{s}
}

func (h statusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	// If writing fails, there's not much we can do.
	_ = h.Status.Marshal(w)
}
