// Copyright 2019 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package tsv

import (
	"fmt"
	"io"
	"reflect"
	"unsafe"
)

// RowWriter writes structs to TSV files using field names or "tsv" tags
// as TSV column headers.
//
// TODO: Consider letting the caller filter or reorder columns.
type RowWriter struct {
	w               Writer
	headerDone      bool
	cachedRowType   reflect.Type
	cachedRowFormat rowFormat
}

// NewRowWriter constructs a writer.
//
// User must call Flush() after last Write().
func NewRowWriter(w io.Writer) *RowWriter {
	return &RowWriter{w: *NewWriter(w)}
}

// Write writes a TSV row containing the values of v's exported fields.
// v must be a pointer to a struct.
//
// On first Write, a TSV header row is written using v's type.
// Subsequent Write()s may pass v of different type, but no guarantees are made
// about consistent column ordering with different types.
func (w *RowWriter) Write(v interface{}) error {
	typ := reflect.TypeOf(v)
	if typ != w.cachedRowType {
		rowFormat, err := parseRowFormat(typ)
		if err != nil {
			return err
		}
		w.cachedRowType = typ
		w.cachedRowFormat = rowFormat
	}
	if !w.headerDone {
		if err := w.writeHeader(); err != nil {
			return err
		}
		w.headerDone = true
	}
	return w.writeRow(v)
}

// Flush flushes all previously-written rows.
func (w *RowWriter) Flush() error {
	return w.w.Flush()
}

func (w *RowWriter) writeHeader() error {
	for _, col := range w.cachedRowFormat {
		w.w.WriteString(col.columnName)
	}
	return w.w.EndLine()
}

func (w *RowWriter) writeRow(v interface{}) error {
	p := unsafe.Pointer(reflect.ValueOf(v).Pointer())
	for _, col := range w.cachedRowFormat {
		switch col.kind {
		case reflect.Bool:
			v := *(*bool)(unsafe.Pointer(uintptr(p) + col.offset))
			if v {
				w.w.WriteString("true")
			} else {
				w.w.WriteString("false")
			}
		case reflect.String:
			v := *(*string)(unsafe.Pointer(uintptr(p) + col.offset))
			w.w.WriteString(v)
		case reflect.Int8:
			v := *(*int8)(unsafe.Pointer(uintptr(p) + col.offset))
			w.w.WriteInt64(int64(v))
		case reflect.Int16:
			v := *(*int16)(unsafe.Pointer(uintptr(p) + col.offset))
			w.w.WriteInt64(int64(v))
		case reflect.Int32:
			v := *(*int32)(unsafe.Pointer(uintptr(p) + col.offset))
			w.w.WriteInt64(int64(v))
		case reflect.Int64:
			v := *(*int64)(unsafe.Pointer(uintptr(p) + col.offset))
			w.w.WriteInt64(int64(v))
		case reflect.Int:
			v := *(*int)(unsafe.Pointer(uintptr(p) + col.offset))
			w.w.WriteInt64(int64(v))
		case reflect.Uint8:
			v := *(*uint8)(unsafe.Pointer(uintptr(p) + col.offset))
			w.w.WriteUint64(uint64(v))
		case reflect.Uint16:
			v := *(*uint16)(unsafe.Pointer(uintptr(p) + col.offset))
			w.w.WriteUint64(uint64(v))
		case reflect.Uint32:
			v := *(*uint32)(unsafe.Pointer(uintptr(p) + col.offset))
			w.w.WriteUint64(uint64(v))
		case reflect.Uint64:
			v := *(*uint64)(unsafe.Pointer(uintptr(p) + col.offset))
			w.w.WriteUint64(uint64(v))
		case reflect.Uint:
			v := *(*uint)(unsafe.Pointer(uintptr(p) + col.offset))
			w.w.WriteUint64(uint64(v))
		case reflect.Float32:
			v := *(*float32)(unsafe.Pointer(uintptr(p) + col.offset))
			w.w.WriteFloat64(float64(v), 'g', -1)
		case reflect.Float64:
			v := *(*float64)(unsafe.Pointer(uintptr(p) + col.offset))
			w.w.WriteFloat64(v, 'g', -1)
		default:
			return fmt.Errorf("unsupported type %v", col.kind)
		}
	}
	return w.w.EndLine()
}
