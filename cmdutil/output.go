// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cmdutil

import (
	"fmt"
	"io"

	"v.io/x/lib/textutil"
)

// WriteWrappedMessage writes the message to the specified io.Writer taking
// care to line wrap it appropriately for the terminal width.
func WriteWrappedMessage(w io.Writer, m string) {
	_, cols, err := textutil.TerminalSize()
	if err != nil {
		fmt.Fprintf(w, "%s", m)
		return
	}
	wrapped := textutil.NewUTF8WrapWriter(w, cols)
	fmt.Fprintf(wrapped, "%s", m)
	wrapped.Flush()
}
