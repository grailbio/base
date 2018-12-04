// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordio

// Utility functions for encoding and parsing keys/values in a header block.

import (
	"encoding/binary"
	"fmt"

	"github.com/grailbio/base/errors"
)

const (
	// Reserved header keywords.

	// KeyTrailer must be set to true when the recordio file contains a trailer.
	// value type: bool
	KeyTrailer = "trailer"

	// KeyTransformer defines transformer functions used to encode blocks.
	KeyTransformer = "transformer"
)

// KeyValue defines one entry stored in a recordio header block
type KeyValue struct {
	// Key is the header key
	Key string
	// Value is the value corresponding to Key. The value must be one of int*,
	// uint*, float*, bool, or string type.
	Value interface{}
}

// ParsedHeader is the result of parsing the recordio header block contents.
type ParsedHeader []KeyValue

const (
	headerTypeBool   uint8 = 1
	headerTypeInt    uint8 = 2
	headerTypeUint   uint8 = 3
	headerTypeString uint8 = 4
	// TODO(saito) Add more types
)

// Helper for encoding key/value pairs into bytes to be stored in a header
// block. Thread compatible.
type headerEncoder struct {
	data []byte
}

func (e *headerEncoder) grow(delta int) {
	cur := len(e.data)
	if cap(e.data) >= cur+delta {
		e.data = e.data[:cur+delta]
	} else {
		tmp := make([]byte, cur+delta, (cur+delta)*2)
		copy(tmp, e.data)
		e.data = tmp
	}
}

func (e *headerEncoder) putUint(v uint64) {
	e.putRawByte(headerTypeUint)
	cur := len(e.data)
	e.grow(binary.MaxVarintLen64)
	n := binary.PutUvarint(e.data[cur:], v)
	e.data = e.data[:cur+n]
}

func (e *headerEncoder) putInt(v int64) {
	e.putRawByte(headerTypeInt)
	cur := len(e.data)
	e.grow(binary.MaxVarintLen64)
	n := binary.PutVarint(e.data[cur:], v)
	e.data = e.data[:cur+n]
}

func (e *headerEncoder) putRawByte(b uint8) {
	cur := len(e.data)
	e.grow(1)
	e.data[cur] = b
	e.data = e.data[:cur+1]
}

func (e *headerEncoder) putBool(v bool) {
	e.putRawByte(headerTypeBool)
	if v {
		e.putRawByte(1)
	} else {
		e.putRawByte(0)
	}
}

func (e *headerEncoder) putString(s string) {
	e.putRawByte(headerTypeString)
	e.putUint(uint64(len(s)))
	cur := len(e.data)
	e.grow(len(s))
	copy(e.data[cur:], s)
	e.data = e.data[:cur+len(s)]
}

func (e *headerEncoder) putKeyValue(key string, v interface{}) error {
	e.putString(key)
	switch v := v.(type) {
	case bool:
		e.putBool(v)
	case uint:
		e.putUint(uint64(v))
	case uint8:
		e.putUint(uint64(v))
	case uint16:
		e.putUint(uint64(v))
	case uint32:
		e.putUint(uint64(v))
	case uint64:
		e.putUint(v)
	case int:
		e.putInt(int64(v))
	case int8:
		e.putInt(int64(v))
	case int16:
		e.putInt(int64(v))
	case int32:
		e.putInt(int64(v))
	case int64:
		e.putInt(v)
	case string:
		e.putString(v)
	default:
		return fmt.Errorf("illegal header type %T", v)
	}
	return nil
}

// Helper for decoding header data produced by headerEncoder.  Thread
// compatible.
type headerDecoder struct {
	err  errors.Once
	data []byte
}

func (d *headerDecoder) getRawByte() uint8 {
	if len(d.data) <= 0 {
		d.err.Set(fmt.Errorf("Failed to read byte in header"))
		return 0
	}
	b := d.data[0]
	d.data = d.data[1:]
	return b
}

func (d *headerDecoder) getRawValue() interface{} {
	vType := d.getRawByte()
	switch vType {
	case headerTypeBool:
		b := d.getRawByte()
		return b != 0
	case headerTypeUint:
		v, n := binary.Uvarint(d.data)
		if n <= 0 {
			d.err.Set(fmt.Errorf("Failed to parse uint"))
			return 0
		}
		d.data = d.data[n:]
		return v
	case headerTypeInt:
		v, n := binary.Varint(d.data)
		if n <= 0 {
			d.err.Set(fmt.Errorf("Failed to parse uint"))
			return 0
		}
		d.data = d.data[n:]
		return v
	case headerTypeString:
		rn := d.getRawValue()
		if err := d.err.Err(); err != nil {
			return ""
		}
		n, ok := rn.(uint64)
		if !ok {
			d.err.Set(fmt.Errorf("failed to read string key"))
			return ""
		}
		if uint64(len(d.data)) < n {
			d.err.Set(fmt.Errorf("header invalid string (%v)", n))
			return ""
		}
		s := string(d.data[:n])
		d.data = d.data[n:]
		return s
	default:
		d.err.Set(fmt.Errorf("illegal header type %T", vType))
		return nil
	}
}

func (h *ParsedHeader) marshal() ([]byte, error) {
	e := headerEncoder{}
	e.putUint(uint64(len(*h)))
	for _, kv := range *h {
		if err := e.putKeyValue(kv.Key, kv.Value); err != nil {
			return nil, err
		}
	}
	return e.data, nil
}

func (h *ParsedHeader) unmarshal(data []byte) error {
	d := headerDecoder{data: data}
	vn := d.getRawValue()
	if err := d.err.Err(); err != nil {
		return err
	}
	n, ok := vn.(uint64)
	if !ok {
		d.err.Set(fmt.Errorf("Failed to read # header entries"))
		return d.err.Err()
	}
	for i := uint64(0); i < n; i++ {
		vkey := d.getRawValue()
		if d.err.Err() != nil {
			break
		}
		key, ok := vkey.(string)
		if !ok {
			d.err.Set(fmt.Errorf("failed to read string key"))
			break
		}
		value := d.getRawValue()
		if d.err.Err() != nil {
			break
		}
		*h = append(*h, KeyValue{key, value})
	}
	return d.err.Err()
}

// HasTrailer checks if the header has a "trailer" entry.
func (h *ParsedHeader) HasTrailer() bool {
	for _, kv := range *h {
		if kv.Key != KeyTrailer {
			continue
		}
		b, ok := kv.Value.(bool)
		if !ok || !b {
			return false
		}
		return true
	}
	return false
}
