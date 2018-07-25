// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package deprecated

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/grailbio/base/recordio/internal"
)

const defaultItemsPerRecord = 4096

// Packer buffers and packs multiple buffers according to the packed recordio
// format. It is used to implement PackedWriter and to enable concurrent
// writing via a ConcurrentPackedWriter.
type Packer struct {
	opts    PackerOpts
	nItems  int
	nBytes  int
	buffers [][]byte // the byte slices to be written, buffers[0] is reserved for hdr.
}

// PackerOpts represents the options accepted by NewPacker.
type PackerOpts struct {
	// Buffers will be used to accumulate the buffers written to the packer
	// rather than the Packer allocating its own storage. However, this
	// supplied slice will be grown when its capacity is exceeded, in which case
	// the underlying array used by the caller will no longer store the
	// buffers being packed. It is left to the caller to allocate a buffer
	// large enough to contain the number of buffers that it writes.
	Buffers [][]byte

	// Transform is called when buffered data is about to be written to a record.
	// It is intended for implementing data transformations such as compression
	// and/or encryption. The Transform function specified here must be
	// reversible by the Transform function in the Scanner.
	Transform func(in [][]byte) (buf []byte, err error)
}

// NewPacker creates a new Packer.
func NewPacker(opts PackerOpts) *Packer {
	var buffers [][]byte
	if opts.Buffers != nil {
		buffers = opts.Buffers
	} else {
		buffers = make([][]byte, 0, defaultItemsPerRecord)
	}
	return &Packer{
		opts:    opts,
		buffers: buffers,
	}
}

// Write implements io.Writer.
func (pbw *Packer) Write(p []byte) (int, error) {
	return pbw.append(p), nil
}

func (pbw *Packer) append(p []byte) int {
	pbw.buffers = append(pbw.buffers, p)
	pbw.nItems++
	pbw.nBytes += len(p)
	return len(p)
}

// Stored returns the number of buffers and bytes currently stored in the
// Packer.
func (pbw *Packer) Stored() (numItems, numBytes int) {
	return pbw.nItems, pbw.nBytes
}

func (pbw *Packer) reset() {
	pbw.buffers = pbw.buffers[0:0]
	pbw.nItems, pbw.nBytes = 0, 0
}

// Pack packs the stored buffers according to the recordio packed record format
// and resets internal state in preparation for being reused. The packed record
// is returned as the hdr and buffers results; dataSize is the sum of the bytes
// in all of the buffers.
func (pbw *Packer) Pack() (hdr []byte, dataSize int, buffers [][]byte, err error) {
	defer pbw.reset()
	if len(pbw.buffers) == 0 {
		// nothing to flush.
		return nil, 0, nil, nil
	}

	// Flush writes all of the currently buffered items to the current
	// record and start a new record. Each item has a byte slice
	// stored in pw.buffers[1:] with pw.buffers[0] being used to
	// point to the header (# items, size of each item...). This avoids
	// having to shuffle the items in pw.buffers to prepend the header
	// when calling WriteSlices.

	// crc32, 1 varint for # items, n for the size of each of n items.
	hdrSize := crc32.Size + (len(pbw.buffers)+1)*binary.MaxVarintLen32
	hdr = make([]byte, hdrSize)

	// Reserve space for the crc32.
	pos := crc32.Size
	// Write the number of items in this record.
	pos += binary.PutUvarint(hdr[pos:], uint64(len(pbw.buffers)))
	// Write the size of each item.
	for _, p := range pbw.buffers {
		pos += binary.PutUvarint(hdr[pos:], uint64(len(p)))
		dataSize += len(p)
	}
	crc := crc32.Checksum(hdr[crc32.Size:pos], internal.IEEECRC)
	// Write the crc back at the start of the header.
	binary.LittleEndian.PutUint32(hdr, crc)

	hdr = hdr[:pos]

	// Apply any transform, note that the sizes are of the items before
	// being transformed, thus the scan transform must be the inverse of
	// the one applied here.
	if tfn := pbw.opts.Transform; tfn != nil {
		transformed, err := tfn(pbw.buffers)
		if err != nil {
			pbw.reset()
			return nil, 0, nil, fmt.Errorf("recordio: transform error: %v", err)
		}
		pbw.buffers = pbw.buffers[0:0]
		pbw.buffers = append(pbw.buffers, transformed)
		dataSize = len(transformed)
	}
	buffers = pbw.buffers
	return
}

// ObjectPacker marshals and buffers objects using a Packer according to the
// recordio packed record format. It is intended to enable concurrent writing
// via a ConcurrentPackedWriter. The objects are intended to be recovered using
// Unpacker and then unmarshaling the byte slices it returns.
type ObjectPacker struct {
	nItems  int
	objects []interface{}
	pwr     *Packer
	marshal MarshalFunc
}

type MarshalFunc func(scratch []byte, v interface{}) ([]byte, error)
type UnmarshalFunc func(data []byte, v interface{}) error

// ObjectPackerOpts represents the options for NewObjectPacker.
type ObjectPackerOpts struct {
	PackerOpts
}

// NewObjectPacker creates a new ObjectPacker. Objects must be large enough
// to store all of the objects marshalled.
func NewObjectPacker(objects []interface{}, fn MarshalFunc, opts ObjectPackerOpts) *ObjectPacker {
	if opts.Buffers == nil {
		opts.Buffers = make([][]byte, 0, len(objects))
	}
	return &ObjectPacker{
		objects: objects,
		pwr:     NewPacker(opts.PackerOpts),
		marshal: fn,
	}
}

// Marshal marshals and buffers the supplied object.
func (mp *ObjectPacker) Marshal(v interface{}) error {
	p, err := mp.marshal(nil, v)
	if err != nil {
		return err
	}
	mp.objects[mp.nItems] = v
	mp.nItems++
	mp.pwr.append(p)
	return nil
}

// Contents returns the current object contents of the packer and the
// Packer that can be used to serialize its contents.
func (mp *ObjectPacker) Contents() ([]interface{}, *Packer) {
	return mp.objects[0:mp.nItems], mp.pwr
}

// UnpackerOpts represents the options accepted by NewUnpacker.
type UnpackerOpts struct {
	// Buffers is used in the same way as by the Packer and PackerOpts.
	Buffers [][]byte

	// Transform is called on the data read from a record to reverse any
	// transformations performed when creating the record. It is intended
	// for decompression, decryption etc.
	Transform func(scratch, in []byte) (out []byte, err error)
}

// Unpacker unpacks the format created by Packer.
type Unpacker struct {
	opts    UnpackerOpts
	buffers [][]byte
	scratch []byte
}

// NewUnpacker creates a new unpacker.
func NewUnpacker(opts UnpackerOpts) *Unpacker {
	return &Unpacker{
		opts:    opts,
		buffers: opts.Buffers,
	}
}

// Unpack unpacks the buffers serialized in buf according to the
// recordio packed format. The slices it returns point to the
// bytes stored in the supplied buffer.
func (up *Unpacker) Unpack(buf []byte) ([][]byte, error) {
	if len(buf) < crc32.Size {
		return nil, fmt.Errorf("recordio: failed to read crc32")
	}
	crc := binary.LittleEndian.Uint32(buf)
	pos := crc32.Size
	nbufs, n := binary.Uvarint(buf[pos:])
	if n <= 0 {
		return nil, fmt.Errorf("recordio: failed to read number of packed items: %v", n)
	}
	pos += n
	sizes := buf[pos:]
	if nbufs > uint64(len(buf)) {
		return nil, fmt.Errorf("recordio: likely corrupt data, number of packed items exceeds the number of bytes in the record (%v > %v)", nbufs, len(buf))
	}
	if up.buffers == nil {
		up.buffers = make([][]byte, 0, nbufs)
	}
	total := 0
	start := pos
	for i := 0; i < int(nbufs); i++ {
		tmp, n := binary.Uvarint(buf[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("recordio: likely corrupt data, failed to read size of packed item %v: %v", i, n)
		}
		total += int(tmp)
		pos += n
	}
	sizes = sizes[:pos-start]
	ncrc := crc32.Checksum(buf[crc32.Size:pos], internal.IEEECRC)
	if crc != ncrc {
		return nil, fmt.Errorf("recordio: likely corrupt data, crc check failed - corrupt packed record header (%v != %v)?", ncrc, crc)
	}
	if tfn := up.opts.Transform; tfn != nil {
		transformed, err := tfn(up.scratch, buf[pos:])
		if err != nil {
			return nil, fmt.Errorf("recordio: transform error: %v", err)
		}
		buf = transformed
		up.scratch = transformed
		pos = 0
	}
	packed := buf[pos:]
	prev := uint64(0)
	max := uint64(len(packed))
	sizePos := 0
	for i := 0; i < int(nbufs)-1; i++ {
		size, n := binary.Uvarint(sizes[sizePos:])
		sizePos += n
		if prev+size > max {
			return nil, fmt.Errorf("recordio: offset greater than buf size (%v > %v), likely due to a mismatched transform or a truncated file", prev+size, max)
		}
		up.buffers = append(up.buffers, packed[prev:prev+size])
		prev += size
	}
	buffers := append(up.buffers, packed[prev:total])
	up.buffers = up.buffers[0:0]
	return buffers, nil
}
