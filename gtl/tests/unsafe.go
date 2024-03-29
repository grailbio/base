// Code generated by "../generate.py --prefix= -DELEM=int32 --package=tests --output=unsafe.go ../unsafe.go.tpl". DO NOT EDIT.

package tests

import (
	"reflect"
	"unsafe"
)

// int32sToBytes casts []int32 to []byte without reallocating.
func int32sToBytes(src []int32) (d []byte) { // nolint: deadcode
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

// BytesToint32s casts []byte to []int32 without reallocating.
func BytesToint32s(src []byte) (d []int32) { // nolint: deadcode
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
