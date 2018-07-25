// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordioutil

import (
	"io"

	"github.com/grailbio/base/recordio"
	"github.com/grailbio/base/recordio/deprecated"
	"github.com/klauspost/compress/flate"
)

// WriterOpts represents the options accepted by NewWriter.
type WriterOpts struct {
	// Marshal is called to marshal an object to a byte slice.
	Marshal recordio.MarshalFunc

	// Index is called whenever a new record is written.
	Index deprecated.RecordIndex

	// MaxItems sets recordio.LegacyPackedWriterOpts when calling NewLegacyPackedWriter
	MaxItems uint32

	// MaxBytes sets recordio.LegacyPackedWriterOpts when calling NewLegacyPackedWriter.
	MaxBytes uint32

	// Flushed is passed to recordio.PackedWriterOpts.
	Flushed func() error

	// FlateLevel indicates the compression level to use, it accepts
	// the values supported by the flate package. The default value (0)
	// is flate.NoCompression.
	// Compression is always performed before encryption.
	FlateLevel int
}

type writer struct {
	deprecated.LegacyPackedWriter
	compressor *FlateTransform
	opts       WriterOpts
}

// NewWriter returns a recordio.LegacyPackedWriter that can optionally compress
// and encrypt the items it writes.
func NewWriter(w io.Writer, opts WriterOpts) (deprecated.LegacyPackedWriter, error) {
	wr := &writer{opts: opts}
	subopts := deprecated.LegacyPackedWriterOpts{
		Marshal:  deprecated.MarshalFunc(opts.Marshal),
		Index:    opts.Index,
		MaxItems: opts.MaxItems,
		MaxBytes: opts.MaxBytes,
		Flushed:  opts.Flushed,
	}

	compress := false
	switch opts.FlateLevel {
	case flate.BestSpeed,
		flate.BestCompression,
		flate.DefaultCompression,
		flate.HuffmanOnly:
		compress = true
	}

	if compress {
		wr.compressor = NewFlateTransform(opts.FlateLevel)
		subopts.Transform = wr.compressor.CompressTransform
	}
	wr.LegacyPackedWriter = deprecated.NewLegacyPackedWriter(w, subopts)
	return wr, nil
}
