// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package logio

func appendUint8(p []byte, v uint8) []byte {
	return append(p, byte(v))
}

func appendUint16(p []byte, v uint16) []byte {
	return append(p, byte(v), byte(v>>8))
}

func appendUint32(p []byte, v uint32) []byte {
	return append(p, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func appendUint64(p []byte, v uint64) []byte {
	return append(p, byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
		byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
}

func appendRecord(p []byte, typ uint8, offset uint64, data []byte) []byte {
	off := len(p)
	p = appendUint32(p, 0)
	p = appendUint8(p, typ)
	p = appendUint16(p, uint16(len(data)))
	p = appendUint64(p, offset)
	p = append(p, data...)
	appendUint32(p[off:off], checksum(p[off+4:]))
	return p
}
