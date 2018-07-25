// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordio_test

import (
	"flag"
	"os"
	"testing"

	"github.com/grailbio/base/grail"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/recordio"
	"github.com/grailbio/base/recordio/deprecated"
	"github.com/grailbio/base/recordio/recordioflate"
	"github.com/grailbio/base/recordio/recordioutil"
	"github.com/grailbio/base/recordio/recordiozstd"
	"github.com/grailbio/testutil/assert"
)

var (
	pathFlag            = flag.String("path", "/tmp/test.recordio", "Recordio file to use during benchmarking")
	numRecordsFlag      = flag.Int("num-records", 3, "Number of records to write")
	recordSizeFlag      = flag.Int("record-size", 64, "Byte size of each record")
	recordsPerBlockFlag = flag.Int("records-per-block", 1024, "Number of records per block")
	fileVersionFlag     = flag.Int("file", 2, "recordio version")
	trailerFlag         = flag.Bool("trailer", true, "Add trailer")
	compressFlag        = flag.String("compress", "zstd", "Compress blocks using the given transformer")
	packFlag            = flag.Bool("pack", true, "Pack (legcy) items")
)

var recordTemplate []byte

func generateRecord(length, seed int) []byte {
	if len(recordTemplate) < length*8 {
		recordTemplate = make([]byte, length*8)
		for i := 0; i < len(recordTemplate); i++ {
			recordTemplate[i] = byte('0' + (i % 64))
		}
	}
	startIndex := seed % (len(recordTemplate) - length + 1)
	return recordTemplate[startIndex : startIndex+length]
}

func init() {
	recordiozstd.Init()
	recordioflate.Init()
}

func BenchmarkRead(b *testing.B) {
	if *pathFlag == "" {
		b.Skip("--path is empty")
		return
	}
	var nRecords, nBytes int64
	for i := 0; i < b.N; i++ {
		nRecords, nBytes = 0, 0
		in, err := os.Open(*pathFlag)
		assert.NoError(b, err)
		r := recordio.NewScanner(in, recordio.ScannerOpts{})
		for r.Scan() {
			nBytes += int64(len(r.Get().([]byte)))
			nRecords++
		}
		assert.NoError(b, r.Err())
		assert.NoError(b, in.Close())
	}
	b.Logf("Read %d records, %d bytes (%f bytes/record)",
		nRecords, nBytes, float64(nBytes)/float64(nRecords))
}

func BenchmarkWrite(b *testing.B) {
	if *pathFlag == "" {
		b.Skip("--path is empty")
		return
	}
	for i := 0; i < b.N; i++ {
		out, err := os.Create(*pathFlag)
		assert.NoError(b, err)

		switch {
		case *fileVersionFlag == 2:
			opts := recordio.WriterOpts{}
			if i == 0 {
				opts.Index = func(loc recordio.ItemLocation, v interface{}) error {
					log.Debug.Printf("Index: item %v, loc %v", string(v.([]uint8)), loc)
					return nil
				}
			}
			if *compressFlag != "" {
				opts.Transformers = []string{*compressFlag}
			}
			rw := recordio.NewWriter(out, opts)
			rw.AddHeader("intflag", 12345)
			rw.AddHeader("uintflag", uint64(12345))
			rw.AddHeader("strflag", "Hello")
			rw.AddHeader("boolflag", true)
			if *trailerFlag {
				rw.AddHeader(recordio.KeyTrailer, true)
			}
			for j := 0; j < *numRecordsFlag; j++ {
				rw.Append(generateRecord(*recordSizeFlag, j))
				if j%*recordsPerBlockFlag == *recordsPerBlockFlag-1 {
					rw.Flush()
				}
			}
			if *trailerFlag {
				rw.SetTrailer([]byte("Trailer"))
			}
			assert.NoError(b, rw.Finish())
		case *fileVersionFlag == 1 && !*packFlag:
			opts := deprecated.LegacyWriterOpts{}
			if *compressFlag != "" {
				panic("Legacy unpacked format does not support --compress flag")
			}
			rw := deprecated.NewLegacyWriter(out, opts)
			for j := 0; j < *numRecordsFlag; j++ {
				n, err := rw.Write(generateRecord(*recordSizeFlag, j))
				assert.NoError(b, err)
				assert.EQ(b, n, *recordSizeFlag)
			}
		case *fileVersionFlag == 1 && *packFlag:
			opts := deprecated.LegacyPackedWriterOpts{}
			switch *compressFlag {
			case "":
			case "flate":
				opts.Transform = recordioutil.NewFlateTransform(-1).CompressTransform
			default:
				panic(*compressFlag)
			}
			rw := deprecated.NewLegacyPackedWriter(out, opts)
			for j := 0; j < *numRecordsFlag; j++ {
				n, err := rw.Write(generateRecord(*recordSizeFlag, j))
				assert.NoError(b, err)
				assert.EQ(b, n, *recordSizeFlag)
				if j%*recordsPerBlockFlag == *recordsPerBlockFlag-1 {
					log.Printf("FLUSH! %d", *recordsPerBlockFlag)
					assert.NoError(b, rw.Flush())
				}
			}
			assert.NoError(b, rw.Flush())
		default:
			panic(*fileVersionFlag)
		}
		assert.NoError(b, out.Close())
	}
}

func TestMain(m *testing.M) {
	shutdown := grail.Init()
	status := m.Run()
	shutdown()
	os.Exit(status)
}
