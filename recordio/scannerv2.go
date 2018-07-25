// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordio

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/recordio/internal"
)

var scannerFreePool = sync.Pool{
	New: func() interface{} {
		return &scannerv2{}
	},
}

// rawItemList is the result of uncompressing & parsing one recordio block.
type rawItemList struct {
	bytes    []byte // raw bytes, post transformation.
	firstOff int    // bytes[firstOff:] contain the application payload
	cumSize  []int  // cumSize[x] is the cumulative bytesize of items [0,x].
}

func (ri *rawItemList) clear() {
	ri.bytes = ri.bytes[:0]
	ri.cumSize = ri.cumSize[:0]
	ri.firstOff = 0
}

// len returns the number of items in the block.
func (ri *rawItemList) len() int { return len(ri.cumSize) }

// item returns the i'th (base 0) item.
//
// REQUIRES: 0 <= i < ri.len().
func (ri *rawItemList) item(i int) []byte {
	startOff := ri.firstOff
	if i > 0 {
		startOff += ri.cumSize[i-1]
	}
	limitOff := ri.firstOff + ri.cumSize[i]
	return ri.bytes[startOff:limitOff]
}

// Given block contents, apply transformation if any, and parse it into a list
// of items. If transform is nil, it defaults to identity.
func parseChunksToItems(rawItems *rawItemList, chunks [][]byte, transform TransformFunc) error {
	if transform == nil {
		// TODO(saito) Allow TransformFunc to return an iov, and refactor the rest
		// of the codebase to consume it.
		transform = idTransform
	}
	var err error
	if rawItems.bytes != nil {
		// zstd doesn't like an empty slice (zstd.go:100)
		//
		// TODO(saito) fix upstream.
		rawItems.bytes = rawItems.bytes[:cap(rawItems.bytes)]
	}
	if rawItems.bytes, err = transform(rawItems.bytes, chunks); err != nil {
		return err
	}
	block := rawItems.bytes
	unItems, n := binary.Uvarint(block)
	if n <= 0 {
		return fmt.Errorf("recordio: failed to read number of packed items: %v", n)
	}
	nItems := int(unItems)
	pos := n

	if cap(rawItems.cumSize) < nItems {
		rawItems.cumSize = make([]int, nItems)
	} else {
		rawItems.cumSize = rawItems.cumSize[:nItems]
	}
	total := 0
	for i := 0; i < nItems; i++ {
		size, n := binary.Uvarint(block[pos:])
		if n <= 0 {
			return fmt.Errorf("recordio: likely corrupt data, failed to read size of packed item %v: %v", i, n)
		}
		total += int(size)
		rawItems.cumSize[i] = total
		pos += n
	}
	rawItems.firstOff = pos
	if total+pos != len(block) {
		return fmt.Errorf("recordio: corrupt block header, got block size %d, expected %d", len(block), total+pos)
	}
	return nil
}

// ScannerOpts defines options used when creating a new scanner.
type ScannerOpts struct {
	// LegacyTransform is used only to read the legacy recordio files. For the V2
	// recordio files, this field is ignored, and transformers are constructed
	// from the header metadata.
	LegacyTransform TransformFunc

	// Unmarshal transforms a byte slice into an application object. It is called
	// for every item read from storage. If nil, a function that returns []byte
	// unchanged is used. The return value from Unmarshal can be retrieved using
	// the Scanner.Get method.
	Unmarshal func(in []byte) (out interface{}, err error)
}

// Scanner defines an interface for recordio scanner.
//
// A Scanner implementation must be thread safe.  Legal path expression is
// defined below. Err, Header, and Trailer can be called at any time.
//
//   ((Scan Get*) | Seek)* Finish
//
type Scanner interface {
	// Header returns the contents of the header block.
	Header() ParsedHeader

	// Scan returns true if a new record was read, false otherwise. It will return
	// false on encountering an error; the error may be retrieved using the Err
	// method. Note, that Scan will reuse storage from one invocation to the next.
	Scan() bool

	// Get returns the current item as read by a prior call to Scan.
	//
	// REQUIRES: Preceding Scan calls have returned true. There is no Seek
	// call between the last Scan call and the Get call.
	Get() interface{}

	// Err returns any error encountered by the writer. Once Err() becomes
	// non-nil, it stays so.
	Err() error

	// Set up so that the next Scan() call causes the pointer to move to the given
	// location.  On any error, Err() will be set.
	//
	// REQUIRES: loc must be one of the values passed to the Index callback
	// during writes.
	Seek(loc ItemLocation)

	// Trailer returns the trailer block contents.  If the trailer does not exist,
	// or is corrupt, it returns nil.  The caller should examine Err() if Trailer
	// returns nil.
	Trailer() []byte

	// Return the file format version. Not for general use.
	Version() FormatVersion

	// Finish should be called exactly once, after the application has finished
	// using the scanner. It returns the value of Err().
	//
	// The Finish method recycles the internal scanner resources for use by other
	// scanners, thereby reducing GC overhead. THe application must not touch the
	// scanner object after Finish.
	Finish() error
}

type scannerv2 struct {
	err         errors.Once
	sc          *internal.ChunkScanner
	opts        ScannerOpts
	untransform TransformFunc
	header      ParsedHeader

	rawItems rawItemList
	item     interface{}
	nextItem int
}

func idUnmarshal(data []byte) (interface{}, error) {
	return data, nil
}

type errorScanner struct {
	err error
}

func (s *errorScanner) Header() (p ParsedHeader)   { return }
func (s *errorScanner) Trailer() (b []byte)        { return }
func (s *errorScanner) Version() (v FormatVersion) { return }
func (s *errorScanner) Get() interface{}           { panic(fmt.Sprintf("errorscannerv2.Get: %v", s.err)) }
func (s *errorScanner) Scan() bool                 { return false }
func (s *errorScanner) Seek(ItemLocation)          {}
func (s *errorScanner) Finish() error              { return s.Err() }
func (s *errorScanner) Err() error {
	if s.err == io.EOF {
		return nil
	}
	return s.err
}

// NewScanner creates a new recordio scanner. The reader can read both legacy
// recordio files (packed or unpacked) or the new-format files. Any error is
// reported through the Scanner.Err method.
func NewScanner(in io.ReadSeeker, opts ScannerOpts) Scanner {
	return NewShardScanner(in, opts, 0, 1, 1)
}

// NewShardScanner creates a new sharded recordio scanner. The returned scanner
// reads shard [start,limit) (of [0,nshard)) of the recordio file at the
// ReadSeeker in.  Sharding is only supported for v2 recordio files; an error
// scanner is returned if NewShardScanner is called for a legacy recordio file.
//
// NewShardScanner with shard and nshard set to 0 and 1 respectively (i.e.,
// a single shard) behaves as NewScanner.
func NewShardScanner(in io.ReadSeeker, opts ScannerOpts, start, limit, nshard int) Scanner {
	if opts.Unmarshal == nil {
		opts.Unmarshal = idUnmarshal
	}
	if err := internal.Seek(in, 0); err != nil {
		return &errorScanner{err}
	}
	var magic internal.MagicBytes
	if _, err := io.ReadFull(in, magic[:]); err != nil {
		return &errorScanner{err}
	}
	if err := internal.Seek(in, 0); err != nil {
		return &errorScanner{err}
	}
	if start >= limit || limit > nshard || start < 0 || nshard <= 0 {
		return &errorScanner{fmt.Errorf("invalid sharding [%d,%d) of %d", start, limit, nshard)}
	}
	if magic != internal.MagicHeader {
		if start != 0 || limit != 1 || nshard != 1 {
			return &errorScanner{errors.New("legacy record IOs do not support sharding")}
		}
		return newLegacyScannerAdapter(in, opts)
	}
	return newScanner(in, start, limit, nshard, opts)
}

func newScanner(in io.ReadSeeker, start, limit, nshard int, opts ScannerOpts) Scanner {
	s := scannerFreePool.Get().(*scannerv2)
	if s == nil {
		panic("newScannerV2")
	}
	s.err = errors.Once{Ignored: []error{io.EOF}}
	s.opts = opts
	s.untransform = nil
	s.header = nil
	s.nextItem = 0
	s.item = nil
	s.sc = internal.NewChunkScanner(in, &s.err)
	s.rawItems.clear()
	s.readHeader()
	if s.Err() != nil {
		return s
	}
	// Technically, we shouldn't be reading the trailer again, but
	// the block scanner just ignores it anyway.
	s.sc.LimitShard(start, limit, nshard)
	return s
}

func (s *scannerv2) readSpecialBlock(expectedMagic internal.MagicBytes, tr TransformFunc) []byte {
	if !s.sc.Scan() {
		s.err.Set(fmt.Errorf("Failed to read block %v", expectedMagic))
		return nil
	}
	magic, chunks := s.sc.Block()
	if magic != expectedMagic {
		s.err.Set(fmt.Errorf("Failed to read block, expect %v, got %v", expectedMagic, magic))
		return nil
	}
	rawItems := rawItemList{}
	err := parseChunksToItems(&rawItems, chunks, tr)
	if err != nil {
		s.err.Set(err)
		return nil
	}
	if rawItems.len() != 1 {
		s.err.Set(fmt.Errorf("Wrong # of items in header block, %d", rawItems.len()))
		return nil
	}
	return rawItems.item(0)
}

func (s *scannerv2) readHeader() {
	payload := s.readSpecialBlock(internal.MagicHeader, idTransform)
	if s.err.Err() != nil {
		return
	}
	if err := s.header.unmarshal(payload); err != nil {
		s.err.Set(err)
		return
	}
	transformers := []string{}
	for _, h := range s.header {
		if h.Key == KeyTransformer {
			str, ok := h.Value.(string)
			if !ok {
				s.err.Set(fmt.Errorf("Expect string value for key %v, but found %v", h.Key, h.Value))
				return
			}
			transformers = append(transformers, str)
		}
	}
	var err error
	s.untransform, err = registry.GetUntransformer(transformers)
	s.err.Set(err)
}

func (s *scannerv2) Version() FormatVersion {
	return V2
}

func (s *scannerv2) Header() ParsedHeader {
	return s.header
}

func (s *scannerv2) Trailer() []byte {
	if !s.header.HasTrailer() {
		return nil
	}
	curOff := s.sc.Tell()
	defer s.sc.Seek(curOff)

	magic, chunks := s.sc.ReadLastBlock()
	if s.err.Err() != nil {
		return nil
	}
	if magic != internal.MagicTrailer {
		s.err.Set(fmt.Errorf("Did not found the trailer, instead found magic %v", magic))
		return nil
	}
	rawItems := rawItemList{}
	err := parseChunksToItems(&rawItems, chunks, s.untransform)
	if err != nil {
		s.err.Set(err)
		return nil
	}
	if rawItems.len() != 1 {
		s.err.Set(fmt.Errorf("Expect exactly one trailer item, but found %d", rawItems.len()))
		return nil
	}
	return rawItems.item(0)
}

func (s *scannerv2) Get() interface{} {
	return s.item
}

func (s *scannerv2) Seek(loc ItemLocation) {
	// TODO(saito) Avoid seeking the file if loc.Block points to the current block.
	if s.err.Err() == io.EOF {
		s.err = errors.Once{}
	}
	s.sc.Seek(int64(loc.Block))
	if !s.scanNextBlock() {
		return
	}
	if loc.Item >= s.rawItems.len() {
		s.err.Set(fmt.Errorf("Invalid location %+v, block has only %d items", loc, s.rawItems.len()))
	}
	s.nextItem = loc.Item
}

func (s *scannerv2) scanNextBlock() bool {
	s.rawItems.clear()
	s.nextItem = 0
	if s.Err() != nil {
		return false
	}
	// Need to read the next record.
	if !s.sc.Scan() {
		return false
	}
	magic, chunks := s.sc.Block()
	if magic == internal.MagicPacked {
		if err := parseChunksToItems(&s.rawItems, chunks, s.untransform); err != nil {
			s.err.Set(err)
			return false
		}
		s.nextItem = 0
		return true
	}
	if magic == internal.MagicTrailer {
		// EOF
		return false
	}
	s.err.Set(fmt.Errorf("recordio: invalid magic number: %v", magic))
	return false
}

func (s *scannerv2) Scan() bool {
	for s.nextItem >= s.rawItems.len() {
		if !s.scanNextBlock() {
			return false
		}
	}
	item, err := s.opts.Unmarshal(s.rawItems.item(s.nextItem))
	if err != nil {
		s.err.Set(err)
		return false
	}
	s.item = item
	s.nextItem++
	return true
}

func (s *scannerv2) Err() error {
	err := s.err.Err()
	if err == io.EOF {
		err = nil
	}
	return err
}

func (s *scannerv2) Finish() error {
	err := s.Err()
	s.err = errors.Once{}
	s.opts = ScannerOpts{}
	s.sc = nil
	s.untransform = nil
	s.header = nil
	s.nextItem = 0
	s.item = nil
	scannerFreePool.Put(s)
	return err
}
