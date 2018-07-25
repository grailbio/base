// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package deprecated

import (
	"fmt"
	"io"
	"sync"

	"github.com/grailbio/base/recordio/internal"
)

var (
	// MaxPackedItems defines the max items that can be
	// packed into a single record by a PackedWriter.
	MaxPackedItems = uint32(10 * 1024 * 1024)
	// DefaultPackedItems defines the default number of items that can
	// be packed into a single record by a PackedWriter.
	DefaultPackedItems = uint32(16 * 1024)
	// DefaultPackedBytes defines the default number of bytes that can
	// be packed into a single record by a PackedWriter.
	DefaultPackedBytes = uint32(16 * 1024 * 1024)
)

// RecordIndex is called every time a new record is added to a stream. It is
// called with the offset and size of the record, and the number of items being
// written to the record. It can optionally return a function that will be
// subsequently called for each item that is written to this record. This makes
// it possibly to ensure that all calls to index the items in a single record
// are handled by the same method and object and hence to index records
// concurrently.
type RecordIndex func(recordOffset, recordLength, nitems uint64) (ItemIndexFunc, error)

// ItemIndexFunc is called every item that an item is added to a record.
type ItemIndexFunc func(itemOffset, itemLength uint64, v interface{}, p []byte) error

// LegacyPackedWriterOpts represents the options to NewPackedWriter
type LegacyPackedWriterOpts struct {
	// Marshal is called to marshal an object to a byte slice.
	Marshal MarshalFunc

	// Index is called whenever a new record is written.
	Index RecordIndex

	// Flushed is called whenever a record is written.
	Flushed func() error

	// Transform is called when buffered data is about to be written to a record.
	// It is intended for implementing data transformations such as compression
	// and/or encryption. The Transform function specified here must be
	// reversible by the Transform function in the Scanner.
	Transform func(in [][]byte) (buf []byte, err error)

	// MaxItems is the maximum number of items to pack into a single record.
	// It defaults to DefaultPackedItems if set to 0.
	// If MaxItems exceeds MaxPackedItems it will silently set to MaxPackedItems.
	MaxItems uint32

	// MaxBytes is the maximum number of bytes to pack into a single record.
	// It defaults to DefaultPackedBytes if set to 0.
	MaxBytes uint32
}

// LegacyPackedScannerOpts represents the options to NewPackedScanner
type LegacyPackedScannerOpts struct {
	LegacyScannerOpts

	// Transform is called on the data read from a record to reverse any
	// transformations performed when creating the record. It is intended
	// for decompression, decryption etc.
	Transform func(scratch, in []byte) (out []byte, err error)
}

// PackedWriter represents an interface that can be used to write multiple
// items to the same recordio record.
type LegacyPackedWriter interface {
	// Write writes a []byte record to the supplied writer. Each call to write
	// results in a new record being written.
	// Calls to Write and Record may be interspersed.
	io.Writer

	// Marshal marshals an object priort to writing it to the underlying
	// recordio stream.
	Marshal(v interface{}) (n int, err error)

	// Flush is called to write any currently buffered data to the current
	// record. A subsequent write will result in a new record being
	// written. Flush must be called to ensure that the last record is
	// completely written.
	Flush() error
}

// PackedScanner represents an interface that can be used to read items
// from a recordio file written using a PackedWriter.
type LegacyPackedScanner interface {
	LegacyScanner
}

type packedWriter struct {
	sync.Mutex
	wr      *byteWriter
	pbw     *Packer
	opts    LegacyPackedWriterOpts
	objects []interface{}
}

// NewLegacyPackedWriter is deprecated. Use NewWriterV2 instead.
//
// NewLegacyPackedWriter is writer that will pack up to MaxItems or MaxBytes,
// whichever comes first, into a single write to the underlying recordio
// stream. Callers to Write must guarantee that they will not modify the buffers
// passed as arguments since Write does not make an internal copy until the
// buffered data is written. A caller can count items/bytes or provide a Flushed
// called to determine when it is safe to reuse any storage.  This scheme avoids
// an unnecessary copy for []byte and most implementations of Marshal will
// create a new buffer to store the marshaled data.
func NewLegacyPackedWriter(wr io.Writer, opts LegacyPackedWriterOpts) LegacyPackedWriter {
	if opts.MaxItems == 0 {
		opts.MaxItems = DefaultPackedItems
	}
	if opts.MaxBytes == 0 {
		opts.MaxBytes = DefaultPackedBytes
	}
	if opts.MaxItems > MaxPackedItems {
		opts.MaxItems = MaxPackedItems
	}
	bufStorage := make([][]byte, 0, opts.MaxItems+1)
	objectStorage := make([]interface{}, 0, opts.MaxItems)
	subopts := PackerOpts{
		Transform: opts.Transform,
		Buffers:   bufStorage, // reserve the first buffer for the hdr.
	}
	pw := &packedWriter{
		opts:    opts,
		wr:      NewLegacyWriter(wr, LegacyWriterOpts{Marshal: opts.Marshal}).(*byteWriter),
		pbw:     NewPacker(subopts),
		objects: objectStorage,
	}
	pw.wr.magic = internal.MagicPacked
	return pw
}

// Implement recordio.LegacyPackedWriter.Write.
func (pw *packedWriter) Write(p []byte) (n int, err error) {
	pw.Lock()
	defer pw.Unlock()
	if err := pw.flushIfNeeded(len(p)); err != nil {
		return 0, err
	}
	pw.objects = append(pw.objects, nil)
	return pw.pbw.Write(p)
}

func (pw *packedWriter) flushIfNeeded(lp int) error {
	if lp > int(pw.opts.MaxBytes) {
		return fmt.Errorf("buffer is too large %v > %v", lp, pw.opts.MaxBytes)
	}
	// lock already held.
	nItems, nBytes := pw.pbw.Stored()
	if ((nBytes + lp) > int(pw.opts.MaxBytes)) || ((nItems + 1) > int(pw.opts.MaxItems)) {
		return pw.flush()
	}
	return nil
}

// Implement recordio.Writer.
func (pw *packedWriter) Marshal(v interface{}) (n int, err error) {
	mfn := pw.opts.Marshal
	if mfn == nil {
		return 0, fmt.Errorf("Marshal function not configured for recordio.PackedWriter")
	}
	p, err := mfn(nil, v)
	if err != nil {
		return 0, err
	}
	pw.Lock()
	defer pw.Unlock()
	if err := pw.flushIfNeeded(len(p)); err != nil {
		return 0, err
	}
	pw.objects = append(pw.objects, v)
	return pw.pbw.Write(p)
}

// Implement recordio.LegacyPackedWriter.Flush.
func (pw *packedWriter) Flush() error {
	pw.Lock()
	defer pw.Unlock()
	return pw.flush()
}

func (pw *packedWriter) flush() error {
	// lock already held.
	stored, _ := pw.pbw.Stored()
	hdr, dataSize, buffers, err := pw.pbw.Pack()
	if err != nil {
		return err
	}
	if len(buffers) == 0 {
		// It's ok to write out buffers of zero length, hence dataSize == 0
		// can't be used to determine if there's no data to write out.
		return nil
	}
	hdrSize, offset, n, err := pw.wr.writeSlices(hdr, buffers...)
	if err != nil {
		return err
	}
	if got, want := n, dataSize+len(hdr); got != want {
		return fmt.Errorf("recordio: buffered write too short wrote %v instead of %v", got, want)
	}

	// Call the indexing funcs.
	if rifn := pw.opts.Index; rifn != nil {
		next := uint64(0)
		ifn, err := rifn(offset, uint64(n)+hdrSize, uint64(stored))
		if err != nil {
			return err
		}
		if ifn != nil {
			for i, b := range buffers {
				err := ifn(next, uint64(len(b)), pw.objects[i], b)
				if err != nil {
					return err
				}
				next += uint64(len(b))
			}
		}
	}
	pw.objects = pw.objects[0:0]
	// Reset everything ready for the next record.
	if flfn := pw.opts.Flushed; flfn != nil {
		return flfn()
	}
	return nil
}

type packedScanner struct {
	err      error
	sc       *LegacyScannerImpl
	buffered [][]byte
	record   []byte
	opts     LegacyPackedScannerOpts
	nextItem int
	pbr      *Unpacker
}

// NewLegacyPackedScanner is deprecated. Use NewScannerV2 instead.
func NewLegacyPackedScanner(rd io.Reader, opts LegacyPackedScannerOpts) LegacyScanner {
	return &packedScanner{
		sc: NewLegacyScanner(rd, opts.LegacyScannerOpts).(*LegacyScannerImpl),
		pbr: NewUnpacker(UnpackerOpts{
			Transform: opts.Transform,
		}),
		opts: opts,
	}
}

func (ps *packedScanner) setErr(err error) {
	if ps.err == nil {
		ps.err = err
	}
}

// Reset implements recordio.Scanner.Reset.
func (ps *packedScanner) Reset(rd io.Reader) {
	ps.sc.Reset(rd)
	ps.nextItem = 0
	ps.buffered = ps.buffered[:0]
	ps.err = nil
}

// Scan implements recordio.Scanner.Scan.
func (ps *packedScanner) Scan() bool {
	if ps.err != nil {
		return false
	}
	if ps.nextItem < len(ps.buffered) {
		ps.record = ps.buffered[ps.nextItem]
		ps.nextItem++
		return true
	}
	// Need to read the next record.
	magic, ok := ps.sc.InternalScan()
	if !ok {
		return false
	}
	if magic != internal.MagicPacked {
		ps.sc.err.Set(fmt.Errorf("recordio: invalid magic number: %v, expect %v", magic, internal.MagicPacked))
		return false
	}
	tmp, err := ps.pbr.Unpack(ps.sc.Bytes())
	if err != nil {
		ps.setErr(err)
		return false
	}
	ps.buffered = tmp
	ps.record = ps.buffered[0]
	ps.nextItem = 1
	return true
}

// Scan implements recordio.Scanner.Bytes.
func (ps *packedScanner) Bytes() []byte {
	return ps.record
}

// Scan implements recordio.Scanner.Err.
func (ps *packedScanner) Err() error {
	if ps.err != nil {
		return ps.err
	}
	return ps.sc.Err()
}

// Scan implements recordio.Scanner.Unmarshal.
func (ps *packedScanner) Unmarshal(v interface{}) error {
	if ufn := ps.opts.Unmarshal; ufn != nil {
		return ufn(ps.Bytes(), v)
	}
	err := fmt.Errorf("Unmarshal function not configured for recordio.PackedScanner")
	ps.setErr(err)
	return err
}
