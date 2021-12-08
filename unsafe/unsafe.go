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
