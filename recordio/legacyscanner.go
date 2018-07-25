// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordio

import (
	"fmt"
	"io"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/recordio/deprecated"
	"github.com/grailbio/base/recordio/internal"
)

// legacyScanner is a ScannerV2 implementation that reads legacy recordio files,
// either packed or unpacked.
type legacyScannerAdapter struct {
	err  errors.Once
	in   io.ReadSeeker
	sc   *deprecated.LegacyScannerImpl
	opts ScannerOpts

	pbr      *deprecated.Unpacker
	buffered [][]byte
	item     interface{}
	nextItem int
}

func newLegacyScannerAdapter(in io.ReadSeeker, opts ScannerOpts) Scanner {
	var legacyTransform func(scratch, in []byte) ([]byte, error)
	if opts.LegacyTransform != nil {
		legacyTransform = func(scratch, in []byte) ([]byte, error) {
			return opts.LegacyTransform(scratch, [][]byte{in})
		}
	}
	return &legacyScannerAdapter{
		in:   in,
		sc:   deprecated.NewLegacyScanner(in, deprecated.LegacyScannerOpts{}).(*deprecated.LegacyScannerImpl),
		opts: opts,
		pbr:  deprecated.NewUnpacker(deprecated.UnpackerOpts{Transform: legacyTransform}),
	}
}

func (s *legacyScannerAdapter) Version() FormatVersion {
	return V1
}

func (s *legacyScannerAdapter) Header() ParsedHeader {
	return ParsedHeader{}
}

func (s *legacyScannerAdapter) Trailer() []byte {
	return nil
}

func (s *legacyScannerAdapter) seekRaw(off int64) bool {
	err := internal.Seek(s.in, off)
	if err != nil {
		s.err.Set(err)
		return false
	}
	s.sc.Reset(s.in)
	return true
}

func (s *legacyScannerAdapter) Seek(loc ItemLocation) {
	// TODO(saito) Avoid seeking the file if loc.Block points to the current block.
	if s.err.Err() == io.EOF {
		s.err = errors.Once{}
	}
	if !s.seekRaw(int64(loc.Block)) {
		return
	}
	if !s.scanNextBlock() {
		return
	}
	if loc.Item >= len(s.buffered) {
		s.err.Set(fmt.Errorf("Invalid location %+v, block has only %d items", loc, len(s.buffered)))
	}
	s.nextItem = loc.Item
}

func (s *legacyScannerAdapter) scanNextBlock() bool {
	s.buffered = s.buffered[:0]
	s.nextItem = 0
	if s.Err() != nil {
		return false
	}
	// Need to read the next record.
	magic, ok := s.sc.InternalScan()
	if !ok {
		return false
	}
	if magic == internal.MagicPacked {
		tmp, err := s.pbr.Unpack(s.sc.Bytes())
		if err != nil {
			s.err.Set(err)
			return false
		}
		s.buffered = tmp
		s.nextItem = 0
		return true
	}
	if magic == internal.MagicLegacyUnpacked {
		if cap(s.buffered) >= 1 {
			s.buffered = s.buffered[:1]
		} else {
			s.buffered = make([][]byte, 1)
		}
		s.buffered[0] = s.sc.Bytes()
		s.nextItem = 0
		return true
	}
	s.err.Set(fmt.Errorf("recordio: invalid magic number: %v", magic))
	return false
}

func (s *legacyScannerAdapter) Scan() bool {
	for s.nextItem >= len(s.buffered) {
		if !s.scanNextBlock() {
			return false
		}
	}
	item, err := s.opts.Unmarshal(s.buffered[s.nextItem])
	if err != nil {
		s.err.Set(err)
		return false
	}
	s.item = item
	s.nextItem++
	return true
}

func (s *legacyScannerAdapter) Err() error {
	err := s.err.Err()
	if err == nil {
		err = s.sc.Err()
	}
	if err == io.EOF {
		err = nil
	}
	return err
}

func (s *legacyScannerAdapter) Get() interface{} {
	return s.item
}

func (s *legacyScannerAdapter) Finish() error {
	return s.Err()
}
