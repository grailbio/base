// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package deprecated_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/grailbio/base/recordio/deprecated"
	"github.com/grailbio/testutil/expect"
)

func TestRecordioIndex(t *testing.T) {
	type indexEntry struct {
		offset, extent uint64
		p              []byte
		v              interface{}
		first, flushed bool
	}
	nItems := 13

	seenOffset := map[uint64]bool{}
	index := []*indexEntry{}
	buf := &bytes.Buffer{}
	wropts := deprecated.LegacyWriterOpts{
		Marshal: recordioMarshal,
		Index: func(offset, extent uint64, v interface{}, p []byte) error {
			if seenOffset[offset] {
				t.Errorf("duplicate index entry")
			}
			seenOffset[offset] = true
			index = append(index, &indexEntry{offset, extent, p, v, true, false})
			return nil
		},
	}
	wr := deprecated.NewLegacyWriter(buf, wropts)

	data := []string{}
	for i := 0; i < nItems; i++ {
		msg := fmt.Sprintf("hello: %v", rand.Int())
		data = append(data, msg)
		wr.Marshal(&TestPB{msg})
	}
	if got, want := len(index), nItems; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}

	underlying := bytes.NewReader(buf.Bytes())
	scopts := deprecated.LegacyScannerOpts{
		Unmarshal: recordioUnmarshal,
	}
	for i, entry := range index {
		br, _ := deprecated.NewRangeReader(underlying, int64(entry.offset), int64(entry.extent))
		sc := deprecated.NewLegacyScanner(br, scopts)
		msg := &TestPB{}
		if !sc.Scan() {
			t.Fatalf("%v: %v", i, sc.Err())
		}
		sc.Unmarshal(msg)
		if got, want := msg, entry.v; !reflect.DeepEqual(got, want) {
			t.Errorf("%v: got %v, want %v", entry.offset, got, want)
		}
	}

	wropts.Marshal = nil
	buf.Reset()
	seenOffset = map[uint64]bool{}
	index = []*indexEntry{}
	wr = deprecated.NewLegacyWriter(buf, wropts)

	raw := [][]byte{}
	for i := 0; i < nItems; i++ {
		msg := fmt.Sprintf("hello: %v", rand.Int())
		p, err := proto.Marshal(&TestPB{msg})
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, p)
	}

	saved := [][]byte{}
	wr.Write(raw[0:1][0])
	saved = append(saved, raw[0:1][0])
	wr.Write(raw[1:2][0])
	saved = append(saved, raw[1:2][0])
	wr.WriteSlices(raw[2], raw[3:10]...)
	saved = append(saved, bytes.Join(raw[2:10], nil))
	wr.WriteSlices(nil, raw[11:nItems]...)
	saved = append(saved, bytes.Join(raw[11:nItems], nil))

	if got, want := len(index), 4; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}

	underlying = bytes.NewReader(buf.Bytes())
	for i, entry := range index {
		br, _ := deprecated.NewRangeReader(underlying, int64(entry.offset), int64(entry.extent))
		sc := deprecated.NewLegacyScanner(br, deprecated.LegacyScannerOpts{})
		if got, want := entry.v, interface{}(nil); got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
		if got := entry.p; got != nil {
			t.Errorf("%d: got %v not nil", i, got)
		}
		if !sc.Scan() {
			t.Fatalf("%v: %v", i, sc.Err())
		}
		if got, want := sc.Bytes(), saved[i]; !bytes.Equal(got, want) {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
	}

	// Packed recordio indices.
	buf.Reset()
	seenOffset = map[uint64]bool{}
	index = []*indexEntry{}
	pwropts := deprecated.LegacyPackedWriterOpts{
		MaxItems: 2,
	}
	pwropts.Marshal = recordioMarshal
	pwropts.Index = func(record, recordSize, nitems uint64) (deprecated.ItemIndexFunc, error) {
		if seenOffset[record] {
			t.Errorf("duplicate index entry")
		}
		if nitems > 2 {
			t.Errorf("too many items")
		}
		seenOffset[record] = true
		index = append(index, &indexEntry{offset: record, extent: recordSize, p: nil, v: nil, first: true, flushed: false})
		return func(offset, extent uint64, v interface{}, p []byte) error {
			index = append(index, &indexEntry{offset: offset, extent: extent, p: p, v: v, first: false, flushed: false})
			return nil
		}, nil
	}

	pwropts.Flushed = func() error {
		index = append(index, &indexEntry{flushed: true})
		return nil
	}

	pwr := deprecated.NewLegacyPackedWriter(buf, pwropts)
	for i := 0; i < nItems; i++ {
		pwr.Marshal(&TestPB{data[i]})
	}
	pwr.Flush()
	nrecords := nItems / int(pwropts.MaxItems)
	if (nItems % int(pwropts.MaxItems)) > 0 {
		nrecords++
	}

	// number of items, number of records, including flushes.
	indexSize := nItems + 2*nrecords
	if got, want := len(index), indexSize; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	underlying = bytes.NewReader(buf.Bytes())
	nfirst := 0
	nflushed := 0
	var sc deprecated.LegacyPackedScanner
	pscopts := deprecated.LegacyPackedScannerOpts{}
	pscopts.Unmarshal = recordioUnmarshal

	// Random access to a record, sequential scan within it.
	for i, entry := range index {
		if entry.first {
			nfirst++
			br, _ := deprecated.NewRangeReader(underlying, int64(entry.offset), int64(entry.extent))
			sc = deprecated.NewLegacyPackedScanner(br, pscopts)
			continue
		}
		if entry.flushed {
			nflushed++
			continue
		}
		if !sc.Scan() {
			t.Fatalf("%v: %v", i, sc.Err())
		}
		if got, want := sc.Bytes(), entry.p; !bytes.Equal(got, want) {
			t.Errorf("%v: got %x, want %x", i, got, want)
		}
		msg := &TestPB{}
		sc.Unmarshal(msg)
		if got, want := msg, entry.v; !reflect.DeepEqual(got, want) {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}
	}

	if got, want := nfirst, nrecords; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := nflushed, nrecords; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	underlying = bytes.NewReader(buf.Bytes())
	record := []byte{}
	// Random access to a record, random access to items within it.
	for i, entry := range index {
		if entry.first {
			br, _ := deprecated.NewRangeReader(underlying, int64(entry.offset), int64(entry.extent))
			sc = deprecated.NewLegacyPackedScanner(br, pscopts)
			if !sc.Scan() {
				t.Fatalf("%v: %v", i, sc.Err())
			}
			record = sc.Bytes()
			continue
		}
		if entry.flushed {
			continue
		}
		item := record[entry.offset : entry.offset+entry.extent]
		if got, want := item, entry.p; !bytes.Equal(got, want) {
			t.Errorf("%v:%v: got %x, want %x", i, entry.offset, got, want)
		}
		msg := &TestPB{}
		recordioUnmarshal(item, msg)
		if got, want := msg, entry.v; !reflect.DeepEqual(got, want) {
			t.Errorf("%v: %v: got %v, want %v", i, entry.offset, got, want)
		}
	}

}

func TestIndexErrors(t *testing.T) {
	buf := &bytes.Buffer{}

	wropts := deprecated.LegacyWriterOpts{
		Index: func(offset, extent uint64, v interface{}, p []byte) error {
			return fmt.Errorf("index oops")
		},
	}
	wr := deprecated.NewLegacyWriter(buf, wropts)
	_, err := wr.Write([]byte("hello"))
	expect.HasSubstr(t, err, "index oops")

	_, err = wr.WriteSlices([]byte("hello"), []byte("world"))
	expect.HasSubstr(t, err, "index oops")

	wropts = deprecated.LegacyWriterOpts{
		Marshal: recordioMarshal,
		Index: func(offset, extent uint64, v interface{}, p []byte) error {
			return fmt.Errorf("index oops")
		},
	}
	wr = deprecated.NewLegacyWriter(buf, wropts)
	_, err = wr.Marshal(&TestPB{"x"})
	expect.HasSubstr(t, err, "index oops")

	wr = deprecated.NewLegacyWriter(buf, wropts)
	_, err = wr.Write([]byte("hello"))
	expect.HasSubstr(t, err, "index oops")

	pwropts := deprecated.LegacyPackedWriterOpts{
		Marshal: recordioMarshal,
		Index: func(record, recordSize, items uint64) (deprecated.ItemIndexFunc, error) {
			if items != 1 {
				t.Errorf("got %v, want 1", items)
			}
			return nil, fmt.Errorf("packed record index oops")
		},
		MaxItems: 1,
	}
	pwr := deprecated.NewLegacyPackedWriter(buf, pwropts)
	_, err = pwr.Marshal(&TestPB{"x"})
	expect.NoError(t, err, "Marshall packed writer")
	err = pwr.Flush()
	expect.HasSubstr(t, err, "packed record index oops")

	pwropts.Index = func(record, recordSize, items uint64) (deprecated.ItemIndexFunc, error) {
		return func(offset, extent uint64, v interface{}, p []byte) error {
			return fmt.Errorf("packed item index oops")
		}, nil
	}
	pwr = deprecated.NewLegacyPackedWriter(buf, pwropts)
	_, err = pwr.Marshal(&TestPB{"x"})
	expect.NoError(t, err, "Marshall packed writer first try")
	_, err = pwr.Marshal(&TestPB{"y"})
	expect.HasSubstr(t, err, "packed item index oops")

	pwropts.Index = nil
	pwropts.Flushed = func() error {
		return fmt.Errorf("packed flush oops")
	}
	pwr = deprecated.NewLegacyPackedWriter(buf, pwropts)
	_, err = pwr.Marshal(&TestPB{"z"})
	expect.NoError(t, err, "Marshall packed writer")
	err = pwr.Flush()
	expect.HasSubstr(t, err, "packed flush oops")
}
