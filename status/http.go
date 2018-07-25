// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package status

import (
	"fmt"
	"net/http"
	"text/tabwriter"
	"time"
)

type statusHandler struct{ *Status }

// Handler returns a HTTP handler that renders a simple plain-text status
// snapshot of s on each request.
func Handler(s *Status) http.Handler {
	return statusHandler{s}
}

func (h statusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	now := time.Now()
	for _, group := range h.Status.Groups() {
		v := group.Value()
		tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		fmt.Fprintf(tw, "%s: %s\n", v.Title, v.Status)
		for _, task := range group.Tasks() {
			v := task.Value()
			elapsed := now.Sub(v.Begin)
			elapsed -= elapsed % time.Second
			fmt.Fprintf(tw, "\t%s:\t%s\t%s\n", v.Title, v.Status, elapsed)
		}
		tw.Flush()
	}
}
