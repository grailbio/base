// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package unsafe

import (
	"reflect"
	"unsafe"
)

// BytesToString casts src to a string without extra memory allocation. The
// string returned by this function shares memory with "src".
func BytesToString(src []byte) (d string) {
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dh := (*reflect.StringHeader)(unsafe.Pointer(&d))
	dh.Data = sh.Data
	dh.Len = sh.Len
	return d
}

// StringToBytes casts src to []byte without extra memory allocation. The data
// returned by this function shares memory with "src".
func StringToBytes(src string) (d []byte) {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&src))
	dh := (*reflect.SliceHeader)(unsafe.Pointer(&d))
	dh.Data = sh.Data
	dh.Len = sh.Len
	dh.Cap = sh.Len
	return d
}

// ExtendBytes extends the given byte slice, without zero-initializing the new
// storage space.  The caller must guarantee that cap(d) >= newLen (using e.g.
// a Grow() call on the parent buffer).
func ExtendBytes(dptr *[]byte, newLen int) {
	// An earlier version of this function returned a new byte slice.  However, I
	// don't see a use case where you'd want to keep the old slice object, so
	// I've changed the function to modify the slice object in-place.
	if cap(*dptr) < newLen {
		panic(newLen)
	}
	dh := (*reflect.SliceHeader)(unsafe.Pointer(dptr))
	dh.Len = newLen
}
