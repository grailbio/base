// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package tsv provides a simple TSV writer which takes care of number->string
// conversions and tabs, and is far more performant than fmt.Fprintf (thanks to
// use of strconv.Append{Uint,Float}).
//
// Usage is similar to bufio.Writer, except that in place of the usual Write()
// method, there are typed WriteString(), WriteUint32(), etc. methods which
// append one field at a time to the current line, and an EndLine() method to
// finish the line.
package tsv
