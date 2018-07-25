package PACKAGE

import (
	"reflect"
	"unsafe"
)

// ZZELEMsToBytes casts []ELEM to []byte without reallocating.
func ZZELEMsToBytes(src []ELEM) (d []byte) { // nolint: deadcode
	if len(src) == 0 {
		return nil
	}
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dh := (*reflect.SliceHeader)(unsafe.Pointer(&d))
	const elemSize = int(unsafe.Sizeof(src[0]))
	dh.Data = sh.Data
	dh.Len = sh.Len * elemSize
	dh.Cap = sh.Cap * elemSize
	return d
}

// ZZBytesToELEMs casts []byte to []ELEM without reallocating.
func ZZBytesToELEMs(src []byte) (d []ELEM) { // nolint: deadcode
	if len(src) == 0 {
		return nil
	}
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&src))
	dh := (*reflect.SliceHeader)(unsafe.Pointer(&d))
	const elemSize = int(unsafe.Sizeof(d[0]))
	dh.Data = sh.Data
	dh.Len = sh.Len / elemSize
	dh.Cap = sh.Cap / elemSize
	return d
}
