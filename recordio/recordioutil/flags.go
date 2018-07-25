// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordioutil

import (
	"flag"
	"fmt"

	"github.com/klauspost/compress/flate"
)

// CompressionLevelFlag represents a command line flag to specify a compression
// level
type CompressionLevelFlag struct {
	// Specified will be true if the flag has been specified on the command line.
	Specified bool
	// Level corresponds to the consts in the flate package
	Level int
}

// Set implements flag.Value.
func (f *CompressionLevelFlag) Set(v string) error {
	switch v {
	case "none":
		f.Level = flate.NoCompression
	case "fastest":
		f.Level = flate.BestSpeed
	case "best":
		f.Level = flate.BestCompression
	case "default":
		f.Level = flate.DefaultCompression
	case "huffman-only":
		f.Level = flate.HuffmanOnly
	default:
		return fmt.Errorf("unrecognised compression option: %v: not one of none, fastest, best, default or huffman-only", v)
	}
	f.Specified = true
	return nil
}

// String implements flag.Value.
func (f *CompressionLevelFlag) String() string {
	switch f.Level {
	case flate.NoCompression:
		return "none"
	case flate.BestSpeed:
		return "fastest"
	case flate.BestCompression:
		return "best"
	case flate.DefaultCompression:
		return "default"
	case flate.HuffmanOnly:
		return "huffman-only"
	}
	panic(fmt.Sprintf("unrecognised compression constant: %v", f.Level))
	return "unknown"
}

// WriterFlags represents the flags required to configure a recordioutil.Writer.
type WriterFlags struct {
	CompressionFlag    CompressionLevelFlag
	MegaBytesPerRecord uint
	ItemsPerRecord     uint
}

// WriterOptsFromFlags determines the WriterOpts to use based on the supplied
// command line flags.
func WriterOptsFromFlags(flags *WriterFlags) WriterOpts {
	return WriterOpts{
		MaxItems:   uint32(flags.ItemsPerRecord),
		MaxBytes:   uint32(flags.MegaBytesPerRecord * 1024 * 1024),
		FlateLevel: flags.CompressionFlag.Level,
	}
}

// RegisterWriterFlags registers the flags in WriterFlags with the supplied
// FlagSet. If the values of ItemsPerRecord, MegaBytesPerRecord or
// CompressionFlag.Level have their go default values then appropriate defaults
// will be used instead.
func RegisterWriterFlags(flag *flag.FlagSet, opts *WriterFlags) {
	if opts.ItemsPerRecord == 0 {
		opts.ItemsPerRecord = 16
	}
	if opts.MegaBytesPerRecord == 0 {
		opts.MegaBytesPerRecord = 1024 * 1024
	}
	if opts.CompressionFlag.Level == 0 {
		opts.CompressionFlag.Level = flate.BestCompression
	}
	flag.Var(&opts.CompressionFlag, "recordio-compression-level", "compression level, one of: none|fastest|best|default|huffman-only")
	flag.UintVar(&opts.MegaBytesPerRecord, "recordio-MiB-per-record", opts.MegaBytesPerRecord, "megabytes per recordio record")
	flag.UintVar(&opts.ItemsPerRecord, "recordio-items-per-record", opts.ItemsPerRecord, "items per recordio record")
}
