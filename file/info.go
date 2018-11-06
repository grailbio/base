// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package file

import (
	"time"
)

// Info represents file metadata.
type Info interface {
	// Size returns the length of the file in bytes for regular files; system-dependent for others
	Size() int64
	// ModTime returns modification time for regular files; system-dependent for others
	ModTime() time.Time

	// TODO: add attributes, in form map[string]interface{}.
}
