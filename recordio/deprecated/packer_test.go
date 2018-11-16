// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package deprecated_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/grailbio/base/recordio/deprecated"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/expect"
)

func expectStats(t *testing.T, depth int, wr *deprecated.Packer, eni, enb int) {
	ni, nb := wr.Stored()
	if got, want := ni, eni; got != want {
		t.Errorf("%v: got %v, want %v", testutil.Caller(depth), got, want)
	}
	if got, want := nb, enb; got != want {
		t.Errorf("%v: got %v, want %v", testutil.Caller(depth), got, want)
	}
}

func countBytes(b [][]byte) int {
	s := 0
	for _, l := range b {
		s += len(l)
	}
	return s
}

func cmpComplete(t *testing.T, wr *deprecated.Packer, rd *deprecated.Unpacker, wBufs [][]byte) {
	wDS := countBytes(wBufs)
	expectStats(t, 2, wr, len(wBufs), wDS)
	hdr, gDS, gBufs, err := wr.Pack()
	if err != nil {
		t.Fatalf("%v: %v", testutil.Caller(1), err)
	}
	if got, want := gDS, wDS; got != want {
		t.Errorf("%v: got %v, want %v", testutil.Caller(1), got, want)
	}
	if got, want := len(gBufs), len(wBufs); got != want {
		t.Errorf("%v: got %v, want %v", testutil.Caller(1), got, want)
	}

	for i, l := range gBufs {
		if got, want := l, wBufs[i]; !bytes.Equal(got, want) {
			t.Errorf("%v: got %s, want %s", testutil.Caller(1), got, want)
		}
	}

	rbuf := bytes.Join(append([][]byte{hdr}, gBufs...), nil)
	rBufs, err := rd.Unpack(rbuf)
	if err != nil {
		t.Fatal(err)
	}

	if got, want := len(rBufs), len(wBufs); got != want {
		t.Errorf("%v: got %v, want %v", testutil.Caller(1), got, want)
	}

	for i, l := range rBufs {
		if got, want := l, wBufs[i]; !bytes.Equal(got, want) {
			t.Errorf("%v: got %s, want %s", testutil.Caller(1), got, want)
		}
	}
}

func TestPacker(t *testing.T) {
	wr := deprecated.NewPacker(deprecated.PackerOpts{})
	rd := deprecated.NewUnpacker(deprecated.UnpackerOpts{})
	// Pack on empty has no effect.
	_, _, _, err := wr.Pack()
	if err != nil {
		t.Fatal(err)
	}
	expectStats(t, 1, wr, 0, 0)
	msg := []string{"hello", "world"}
	bufs := [][]byte{}
	for _, d := range msg {
		wr.Write([]byte(d))
		bufs = append(bufs, []byte(d))
	}

	expectStats(t, 1, wr, 2, 10)
	cmpComplete(t, wr, rd, bufs)
	expectStats(t, 1, wr, 0, 0)
	// Pack is not idempotent.
	hdr, _, _, err := wr.Pack()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(hdr), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	expectStats(t, 1, wr, 0, 0)

	msg = []string{"and", "again", "hello", "there"}
	bufs = [][]byte{}
	for _, d := range msg {
		wr.Write([]byte(d))
		bufs = append(bufs, []byte(d))
	}
	expectStats(t, 1, wr, 4, 18)
	cmpComplete(t, wr, rd, bufs)
}

func TestPackerReuse(t *testing.T) {
	buffers := make([][]byte, 1, 3)
	wr := deprecated.NewPacker(deprecated.PackerOpts{
		Buffers: buffers[1:],
	})
	msg := []string{"hello", "world"}
	for _, d := range msg {
		wr.Write([]byte(d))
	}
	hdr, size, bufs, _ := wr.Pack()

	record := bytes.Join(append([][]byte{hdr}, bufs...), nil)

	buffers[0] = hdr
	buffers = buffers[:len(bufs)+1]
	if got, want := hdr, buffers[0]; !bytes.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	nsize := 0
	for i, b := range bufs {
		if got, want := b, buffers[i+1]; !bytes.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
		nsize += len(b)
	}
	if got, want := size, nsize; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cap(bufs)+1, cap(buffers); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	rdbuffers := make([][]byte, 0, 2)
	rd := deprecated.NewUnpacker(deprecated.UnpackerOpts{
		Buffers: rdbuffers,
	})
	bufs, _ = rd.Unpack(record)

	if got, want := cap(bufs), cap(rdbuffers); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	rdbuffers = rdbuffers[:len(bufs)]
	for i, b := range bufs {
		if got, want := b, rdbuffers[i]; !bytes.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
		nsize += len(b)
	}

	// If the number of buffers written exceeds the capacity of
	// the originally supplied Buffers slice, a new one will be
	// created and used by append.
	msg = []string{"hello", "world", "oh", "the", "buffer", "grows"}
	for _, d := range msg {
		wr.Write([]byte(d))
	}
	hdr, _, bufs, _ = wr.Pack()
	record = bytes.Join(append([][]byte{hdr}, bufs...), nil)

	if got, want := cap(bufs), cap(buffers); got <= want {
		t.Errorf("got %v, want > %v", got, want)
	}

	// unpack will create a new slice too.
	bufs, _ = rd.Unpack(record)
	if got, want := cap(bufs), cap(rdbuffers); got <= want {
		t.Errorf("got %v, want > %v", got, want)
	}

}

func TestPackerTransform(t *testing.T) {
	// Prepend __ and append ++ to every record.
	wropts := deprecated.PackerOpts{
		Transform: func(bufs [][]byte) ([]byte, error) {
			r := []byte("__")
			for _, b := range bufs {
				r = append(r, b...)
			}
			return append(r, []byte("++")...), nil
		},
	}
	wr := deprecated.NewPacker(wropts)
	data := []string{"Hello", "World", "How", "Are", "You?"}
	for _, d := range data {
		wr.Write([]byte(d))
	}
	hdr, _, bufs, _ := wr.Pack()
	record := bytes.Join(append([][]byte{hdr}, bufs...), nil)

	// Flattening out the buffers and prepending __ and appending ++
	if got, want := string(bytes.Join(bufs, nil)), "__"+strings.Join(data, "")+"++"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// Scan with the __ and ++ in place. Each item in the each
	// record is the same original size, but the contents are
	// 'shifted' by the leading__
	expected := []string{"__Hel", "loWor", "ldH", "owA", "reYo"}

	rdopts := deprecated.UnpackerOpts{}
	rd := deprecated.NewUnpacker(rdopts)
	read, _ := rd.Unpack(record)

	if got, want := len(expected), len(expected); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	for i, r := range read {
		if got, want := string(r), expected[i]; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
	}

	rdopts = deprecated.UnpackerOpts{
		// Strip the leading ++ and trailing ++ while scanning
		Transform: func(scratch, buf []byte) ([]byte, error) {
			return buf[2 : len(buf)-2], nil
		},
	}
	rd = deprecated.NewUnpacker(rdopts)
	read, _ = rd.Unpack(record)
	if got, want := len(read), len(data); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	for i, r := range read {
		if got, want := string(r), data[i]; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
	}
}

func TestPackerTransformErrors(t *testing.T) {
	wropts := deprecated.PackerOpts{
		Transform: func(bufs [][]byte) ([]byte, error) {
			return nil, fmt.Errorf("transform oops")
		},
	}
	wr := deprecated.NewPacker(wropts)
	wr.Write([]byte("oh"))
	_, _, _, err := wr.Pack()
	expect.HasSubstr(t, err, "transform oops")

	wropts.Transform = nil
	wr = deprecated.NewPacker(wropts)
	wr.Write([]byte("oh"))
	wr.Write([]byte("ah"))
	hdr, _, bufs, _ := wr.Pack()

	record := bytes.Join(append([][]byte{hdr}, bufs...), nil)

	rdopts := deprecated.UnpackerOpts{}
	rdopts.Transform = func(scratch, buf []byte) ([]byte, error) {
		return nil, fmt.Errorf("transform oops")
	}

	rd := deprecated.NewUnpacker(rdopts)
	_, err = rd.Unpack(record)
	expect.HasSubstr(t, err, "transform oops")

	rdopts.Transform = func(scratch, buf []byte) ([]byte, error) {
		return nil, nil
	}

	rd = deprecated.NewUnpacker(rdopts)
	_, err = rd.Unpack(record)
	expect.HasSubstr(t, err, "offset greater than buf size")
}

func TestPackerErrors(t *testing.T) {
	wr := deprecated.NewPacker(deprecated.PackerOpts{})
	msg := []string{"hello", "world"}
	for _, d := range msg {
		wr.Write([]byte(d))
	}
	hdr, _, bufs, _ := wr.Pack()
	record := bytes.Join(append([][]byte{hdr}, bufs...), nil)

	shortReadError := func(offset int, msg string) {
		rd := deprecated.NewUnpacker(deprecated.UnpackerOpts{})
		_, err := rd.Unpack(record[:offset])
		expect.HasSubstr(t, err, msg)
	}
	shortReadError(1, "failed to read crc32")
	shortReadError(4, "failed to read number of packed items")
	shortReadError(5, "likely corrupt data, failed to read size of packed item")
	shortReadError(10, "offset greater than buf size")

	corruptionError := func(offset int, msg string, ow ...byte) {
		tmp := make([]byte, len(record))
		copy(tmp, record)
		for i, v := range ow {
			tmp[offset+i] = v
		}
		rd := deprecated.NewUnpacker(
			deprecated.UnpackerOpts{})
		_, err := rd.Unpack(tmp)
		expect.HasSubstr(t, err, msg)
	}
	tmp := record[2]
	corruptionError(2, "crc check failed - corrupt packed record header", tmp+1)
	corruptionError(4, "likely corrupt data, number of packed items exceeds", 0x7f)
	corruptionError(4, "likely corrupt data, failed to read size of packed item", 0x0f)
	corruptionError(5, "crc check failed - corrupt packed record header", 0x7f)
}

func TestObjectPacker(t *testing.T) {
	objects := make([]interface{}, 1000)
	op := deprecated.NewObjectPacker(objects, recordioMarshal, deprecated.ObjectPackerOpts{})
	op.Marshal(&TestPB{"hello"})
	op.Marshal(&TestPB{"world"})
	objs, _ := op.Contents()
	if got, want := len(objs), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := objs[1].(*TestPB).Message, "world"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := objects[0].(*TestPB).Message, "hello"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestObjectPackerErrors(t *testing.T) {
	// ObjectPacker is tested in concurrent_test
	objects := make([]interface{}, 1000)
	op := deprecated.NewObjectPacker(objects, func(scratch []byte, v interface{}) ([]byte, error) {
		return nil, fmt.Errorf("marshal oops")
	}, deprecated.ObjectPackerOpts{})
	err := op.Marshal(&TestPB{"hello"})
	expect.HasSubstr(t, err, "marshal oops")
}
