// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordio_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/grailbio/base/recordio"
	"github.com/grailbio/base/recordio/deprecated"
	"github.com/grailbio/base/recordio/recordioiov"
	"github.com/grailbio/base/recordio/recordiozstd"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/expect"
)

func init() { recordiozstd.Init() }

// The recordio chunk size
const chunkSize = 32 << 10

func marshalString(scratch []byte, v interface{}) ([]byte, error) {
	return []byte(v.(string)), nil
}

func unmarshalString(data []byte) (interface{}, error) {
	return string(data), nil
}

func readAllV2(t *testing.T, buf *bytes.Buffer) (recordio.ParsedHeader, []string, string) {
	sc := recordio.NewScanner(bytes.NewReader(buf.Bytes()), recordio.ScannerOpts{
		Unmarshal: unmarshalString,
	})
	header := sc.Header()
	trailer := string(sc.Trailer())
	var body []string
	for sc.Scan() {
		body = append(body, sc.Get().(string))
	}
	expect.False(t, sc.Scan()) // Scan() calls after EOF should return false.
	expect.NoError(t, sc.Err())
	return header, body, trailer
}

// Test reading a packed v1 file.
func TestReadV1Packed(t *testing.T) {
	buf := &bytes.Buffer{}
	w := deprecated.NewLegacyPackedWriter(buf, deprecated.LegacyPackedWriterOpts{})
	w.Write([]byte("Foo"))
	w.Write([]byte("Baz"))
	w.Flush()

	_, body, trailer := readAllV2(t, buf)
	expect.EQ(t, "", trailer)
	expect.EQ(t, []string{"Foo", "Baz"}, body)
}

// Test reading an unpacked v1 file.
func TestReadV1Unpacked(t *testing.T) {
	buf := &bytes.Buffer{}
	w := deprecated.NewLegacyWriter(buf, deprecated.LegacyWriterOpts{})
	w.Write([]byte("Foo"))
	w.Write([]byte("Baz"))

	_, body, trailer := readAllV2(t, buf)
	expect.EQ(t, "", trailer)
	expect.EQ(t, []string{"Foo", "Baz"}, body)
}

func TestEmptyFile(t *testing.T) {
	_, body, trailer := readAllV2(t, &bytes.Buffer{})
	expect.EQ(t, "", trailer)
	expect.EQ(t, []string(nil), body)
}

func TestEmptyBody(t *testing.T) {
	buf := &bytes.Buffer{}
	wr := recordio.NewWriter(buf, recordio.WriterOpts{Marshal: marshalString})
	assert.NoError(t, wr.Finish())
	assert.EQ(t, len(buf.Bytes()), chunkSize) // one header chunk
	header, body, trailer := readAllV2(t, buf)
	assert.EQ(t, recordio.ParsedHeader(nil), header)
	assert.EQ(t, []string(nil), body)
	assert.EQ(t, "", trailer)
}

func TestFlushEmpty(t *testing.T) {
	buf := &bytes.Buffer{}
	wr := recordio.NewWriter(buf, recordio.WriterOpts{Marshal: marshalString})
	wr.Flush()
	assert.NoError(t, wr.Finish())
	header, body, trailer := readAllV2(t, buf)
	assert.EQ(t, recordio.ParsedHeader(nil), header)
	assert.EQ(t, []string(nil), body)
	assert.EQ(t, "", trailer)
}

func TestV2NonEmptyHeaderEmptyBody(t *testing.T) {
	buf := &bytes.Buffer{}
	wr := recordio.NewWriter(buf, recordio.WriterOpts{Marshal: marshalString})
	wr.AddHeader("Foo", "Hah")
	assert.NoError(t, wr.Finish())
	assert.EQ(t, len(buf.Bytes()), chunkSize) // one header chunk
	header, body, trailer := readAllV2(t, buf)
	assert.EQ(t, recordio.ParsedHeader{recordio.KeyValue{"Foo", "Hah"}}, header)
	assert.EQ(t, []string(nil), body)
	assert.EQ(t, "", trailer)
}

func TestV2EmptyBodyNonEmptyTrailer(t *testing.T) {
	buf := &bytes.Buffer{}
	wr := recordio.NewWriter(buf, recordio.WriterOpts{Marshal: marshalString})
	wr.AddHeader(recordio.KeyTrailer, true)
	wr.SetTrailer([]byte("TTT"))
	assert.NoError(t, wr.Finish())
	assert.EQ(t, len(buf.Bytes()), 2*chunkSize) // header+trailer
	header, body, trailer := readAllV2(t, buf)
	assert.EQ(t, recordio.ParsedHeader{recordio.KeyValue{recordio.KeyTrailer, true}}, header)
	assert.EQ(t, []string(nil), body)
	assert.EQ(t, "TTT", trailer)
}

func TestV2LargeTrailer(t *testing.T) {
	buf := &bytes.Buffer{}
	wr := recordio.NewWriter(buf, recordio.WriterOpts{Marshal: marshalString})
	wr.AddHeader(recordio.KeyTrailer, true)
	wr.Append("XX")

	rnd := rand.New(rand.NewSource(0))
	largeData := randomString(chunkSize*10+100, rnd)
	wr.SetTrailer([]byte(largeData))
	assert.NoError(t, wr.Finish())
	header, body, trailer := readAllV2(t, buf)
	assert.EQ(t, recordio.ParsedHeader{recordio.KeyValue{recordio.KeyTrailer, true}}, header)
	assert.EQ(t, []string{"XX"}, body)
	assert.EQ(t, largeData, trailer)
}

func TestV2WriteRead(t *testing.T) {
	buf := &bytes.Buffer{}

	index := make(map[string]recordio.ItemLocation)
	wr := recordio.NewWriter(buf, recordio.WriterOpts{
		Marshal: marshalString,
		Index: func(loc recordio.ItemLocation, v interface{}) error {
			index[v.(string)] = loc
			return nil
		},
	})
	wr.AddHeader(recordio.KeyTrailer, true)
	wr.AddHeader("hh0", "vv0")
	wr.AddHeader("hh1", 12345)
	wr.AddHeader("hh2", uint16(234))
	wr.Append("F0")
	wr.Append("F1")
	wr.Flush()
	wr.Append("F2")
	wr.Flush()
	wr.Append("F3")
	wr.SetTrailer([]byte("Trailer2"))
	assert.NoError(t, wr.Finish())

	header, body, trailer := readAllV2(t, buf)
	expect.EQ(t, recordio.ParsedHeader{
		recordio.KeyValue{"trailer", true},
		recordio.KeyValue{"hh0", "vv0"},
		recordio.KeyValue{"hh1", int64(12345)},
		recordio.KeyValue{"hh2", uint64(234)},
	}, header)
	expect.EQ(t, "Trailer2", trailer)
	expect.EQ(t, []string{"F0", "F1", "F2", "F3"}, body)

	// Test seeking
	expect.EQ(t, 4, len(index))
	sc := recordio.NewScanner(bytes.NewReader(buf.Bytes()), recordio.ScannerOpts{
		Unmarshal: unmarshalString,
	})

	for _, value := range body {
		loc := index[value]
		sc.Seek(loc)
		expect.NoError(t, sc.Err())
		expect.True(t, sc.Scan())
		expect.EQ(t, value, sc.Get().(string))
	}
}

func TestV2NonExistentTransformer(t *testing.T) {
	buf := &bytes.Buffer{}
	wr := recordio.NewWriter(buf, recordio.WriterOpts{
		Marshal:      marshalString,
		Transformers: []string{"nonexistent"},
	})
	for i := 0; i < 1000; i++ {
		wr.Append("data")
		wr.Flush()
	}
	wr.Finish()
	assert.Regexp(t, wr.Err(), "Transformer .* not found")
}

func TestV2TransformerError(t *testing.T) {
	// A transformer that adds N to every byte.
	recordio.RegisterTransformer("error",
		func(config string) (recordio.TransformFunc, error) {
			return func(scratch []byte, in [][]byte) ([]byte, error) {
				return nil, fmt.Errorf("synthetic transformer error")
			}, nil
		},
		func(config string) (recordio.TransformFunc, error) {
			t.Fail()
			return nil, nil
		})
	buf := &bytes.Buffer{}
	wr := recordio.NewWriter(buf, recordio.WriterOpts{
		Marshal:      marshalString,
		Transformers: []string{"error"},
	})
	wr.Append("data")
	wr.Finish()
	assert.Regexp(t, wr.Err(), "synthetic transformer error")
}

func TestV2Transformer(t *testing.T) {
	bytewiseTransform := func(scratch []byte, in [][]byte, tr func(uint8) uint8) ([]byte, error) {
		nBytes := recordioiov.TotalBytes(in)
		out := recordioiov.Slice(scratch, nBytes)
		n := 0
		for _, buf := range in {
			for i := range buf {
				out[n] = tr(buf[i])
				n++
			}
		}
		return out, nil
	}
	var nPlus, nMinus, nXor int32

	// A transformer that adds N to every byte.
	recordio.RegisterTransformer("testplus",
		func(config string) (recordio.TransformFunc, error) {
			delta, err := strconv.Atoi(config)
			if err != nil {
				return nil, err
			}
			return func(scratch []byte, in [][]byte) ([]byte, error) {
				atomic.AddInt32(&nPlus, 1)
				return bytewiseTransform(scratch, in, func(b uint8) uint8 { return b + uint8(delta) })
			}, nil
		},
		func(config string) (recordio.TransformFunc, error) {
			delta, err := strconv.Atoi(config)
			if err != nil {
				return nil, err
			}
			return func(scratch []byte, in [][]byte) ([]byte, error) {
				atomic.AddInt32(&nMinus, 1)
				return bytewiseTransform(scratch, in, func(b uint8) uint8 { return b - uint8(delta) })
			}, nil
		})

	// A transformer that xors every byte.
	xorTransformerFactory := func(config string) (recordio.TransformFunc, error) {
		delta, err := strconv.Atoi(config)
		if err != nil {
			return nil, err
		}
		return func(scratch []byte, in [][]byte) ([]byte, error) {
			atomic.AddInt32(&nXor, 1)
			return bytewiseTransform(scratch, in, func(b uint8) uint8 { return b ^ uint8(delta) })
		}, nil
	}
	recordio.RegisterTransformer("testxor", xorTransformerFactory, xorTransformerFactory)

	buf := &bytes.Buffer{}
	wr := recordio.NewWriter(buf, recordio.WriterOpts{
		Marshal:      marshalString,
		Transformers: []string{"testplus 3", "testxor 111"},
	})

	wr.AddHeader(recordio.KeyTrailer, true)
	wr.Append("F0")
	wr.Append("F1")
	wr.Flush()
	wr.Append("F2")
	wr.SetTrailer([]byte("Trailer2"))
	assert.NoError(t, wr.Finish())
	assert.EQ(t, int32(3), nPlus) // two data + one trailer block
	assert.EQ(t, int32(3), nXor)

	header, body, _ := readAllV2(t, buf)
	expect.EQ(t, recordio.ParsedHeader{
		recordio.KeyValue{"transformer", "testplus 3"},
		recordio.KeyValue{"transformer", "testxor 111"},
		recordio.KeyValue{"trailer", true},
	}, header)
	expect.EQ(t, []string{"F0", "F1", "F2"}, body)
	assert.EQ(t, int32(3), nPlus)
	assert.EQ(t, int32(6), nXor)
}

func randomString(n int, r *rand.Rand) string {
	buf := make([]byte, n)
	for i := 0; i < n; i++ {
		buf[i] = uint8('A' + r.Intn(64))
	}
	return string(buf)
}

func generateRandomRecordio(t *testing.T, rnd *rand.Rand, flushProbability float64, nRecords, datasize int, wopts recordio.WriterOpts) ([]byte, []string, map[string]recordio.ItemLocation) {
	buf := &bytes.Buffer{}
	items := make([]string, nRecords)
	index := make(map[string]recordio.ItemLocation)
	wopts.Marshal = marshalString
	wopts.Index = func(loc recordio.ItemLocation, v interface{}) error {
		index[v.(string)] = loc
		return nil
	}
	wr := recordio.NewWriter(buf, wopts)
	wr.AddHeader(recordio.KeyTrailer, true)
	for i := 0; i < nRecords; i++ {
		data := randomString(rnd.Intn(datasize)+1, rnd)
		wr.Append(data)
		items[i] = data
		if rnd.Float64() < flushProbability {
			wr.Flush()
		}
		assert.NoError(t, wr.Err())
	}
	wr.SetTrailer([]byte("Trailer"))
	assert.NoError(t, wr.Finish())
	return buf.Bytes(), items, index
}

func doShardedReads(t *testing.T, data []byte, stride, nshard int, items []string) int {
	expected := items
	var maxShardSize int
	for shard := 0; shard < nshard; shard += stride {
		limit := shard + stride
		if limit > nshard {
			limit = nshard
		}
		ropts := recordio.ScannerOpts{Unmarshal: unmarshalString}
		sc := recordio.NewShardScanner(bytes.NewReader(data), ropts, shard, limit, nshard)
		assert.EQ(t, "Trailer", string(sc.Trailer()), "Error: %v, shard %d/%d", sc.Err(), shard, nshard)
		shardSize := 0
		i := 0
		for sc.Scan() {
			assert.EQ(t, expected[0], sc.Get().(string), "i=%d, err %v, shard %d/%d", i, sc.Err(), shard, nshard)
			expected = expected[1:]
			shardSize++
			i++
		}
		assert.NoError(t, sc.Err())
		if shardSize > maxShardSize {
			maxShardSize = shardSize
		}
	}
	assert.EQ(t, 0, len(expected))
	return maxShardSize
}

func doRandomTest(
	t *testing.T,
	seed int64,
	flushProbability float64,
	nshard int,
	maxrecords int,
	datasize int,
	wopts recordio.WriterOpts) {
	t.Run("r", func(t *testing.T) {
		t.Parallel()
		t.Logf("Start test with wopt %+v, nshards %d, maxrecords %d, datasize %d", wopts, nshard, maxrecords, datasize)

		rnd := rand.New(rand.NewSource(seed))
		var nRecords int
		if maxrecords > 0 {
			nRecords = rnd.Intn(maxrecords) + 1
		}
		data, items, index := generateRandomRecordio(t, rnd, flushProbability, nRecords, datasize, wopts)

		doShardedReads(t, data, 1, nshard, items)

		ropts := recordio.ScannerOpts{Unmarshal: unmarshalString}
		sc := recordio.NewScanner(bytes.NewReader(data), ropts)
		for _, value := range items {
			loc := index[value]
			sc.Seek(loc)
			expect.NoError(t, sc.Err())
			expect.True(t, sc.Scan())
			expect.EQ(t, value, sc.Get().(string))
		}
	})
}

func TestV2Random(t *testing.T) {
	const (
		maxrecords = 2000
		datasize   = 30
	)
	for wo := 0; wo < 2; wo++ {
		opts := recordio.WriterOpts{}
		if wo == 1 {
			opts.Transformers = []string{"zstd"}
		}
		doRandomTest(t, 0, 0.001, 2000, maxrecords, 10<<10, opts)
		doRandomTest(t, 0, 0.1, 1, maxrecords, datasize, opts)
		doRandomTest(t, 0, 1.0, 1, maxrecords, datasize, opts)
		doRandomTest(t, 0, 0.0, 1, maxrecords, datasize, opts)

		opts.MaxFlushParallelism = 1
		doRandomTest(t, 0, 0.1, 1, maxrecords, datasize, opts)
		opts.MaxFlushParallelism = 0
		doRandomTest(t, 0, 0.1, 1000, maxrecords, datasize, opts)
		doRandomTest(t, 0, 1.0, 3, maxrecords, 30, opts)
		doRandomTest(t, 0, 0.0, 2, maxrecords, 30, opts)
		// Make sure we generate blocks big enough so that
		// shards have to straddle block boundaries.
		// Make sure that lots of shards with a single record reads correctly.
		doRandomTest(t, 0, 0.001, 2000, 1, datasize, opts)
		// Same with an empty recordio file.
		doRandomTest(t, 0, 0.001, 2000, 0, datasize, opts)
	}
}

func TestRandomLargeWrites(t *testing.T) {
	rnd := rand.New(rand.NewSource(0))

	nRecords := 100000
	data, items, _ := generateRandomRecordio(t, rnd, 0.01, nRecords, 1024, recordio.WriterOpts{})

	nShards := 10
	maxShardSize := doShardedReads(t, data, 1, nShards, items)
	assert.GT(t, maxShardSize, 8000, "max %d, nshard %d nRecords %d", maxShardSize, nShards, nRecords)
	assert.LT(t, maxShardSize, 12000, "max %d, nshard %d nRecords %d", maxShardSize, nShards, nRecords)

	// Use the same sharding, but use a large absolute shard value to detect possible rounding errors.
	nShards = 1000000000
	stride := nShards / 10
	maxShardSize = doShardedReads(t, data, stride, nShards, items)
	assert.GT(t, maxShardSize, 8000, "max %d, nshard %d nRecords %d", maxShardSize, nShards, nRecords)
	assert.LT(t, maxShardSize, 12000, "max %d, nshard %d nRecords %d", maxShardSize, nShards, nRecords)
}

// TODO: test seeking to bogus location.

// TODO: test flushing with no data.

// TODO: benchmark
