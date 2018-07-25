// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package deprecated_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/recordio/deprecated"
	"github.com/grailbio/base/recordio/internal"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/expect"
)

func TestPackedWriteRead(t *testing.T) {
	sl := func(s ...string) [][]byte {
		var bs [][]byte
		for _, t := range s {
			bs = append(bs, []byte(t))
		}
		return bs
	}
	read := func(sc deprecated.LegacyScanner) string {
		if !sc.Scan() {
			assert.NoError(t, sc.Err())
			return "eof"
		}
		return string(sc.Bytes())
	}

	for _, tc := range []struct {
		in                 [][]byte
		maxItems, maxBytes uint32
		nreads             int
	}{
		{sl(""), 1, 10, 1},
		{sl("a", "b"), 2, 10, 1},
		{sl("", "", ""), 2, 10, 2},
		{sl("hello", "world", "line", "2", "line3"), 2, 100, 3},
		{sl("a", "b", "c", "d", "e", "f", "g"), 100, 2, 4},
	} {
		out := &bytes.Buffer{}
		nflushs := 0
		wropts := deprecated.LegacyPackedWriterOpts{
			MaxItems: tc.maxItems,
			MaxBytes: tc.maxBytes,
			Flushed:  func() error { nflushs++; return nil },
		}
		wr := deprecated.NewLegacyPackedWriter(out, wropts)

		for _, p := range tc.in {
			n, err := wr.Write(p)
			assert.NoError(t, err)
			assert.True(t, n == len(p))
		}
		assert.NoError(t, wr.Flush())
		// Make sure Flushing with nothing to flush has no effect.
		assert.NoError(t, wr.Flush())
		assert.EQ(t, bytes.Count(out.Bytes(), internal.MagicPacked[:]), tc.nreads)
		assert.EQ(t, nflushs, tc.nreads)
		assert.EQ(t, bytes.Count(out.Bytes(), internal.MagicLegacyUnpacked[:]), 0)

		sc := deprecated.NewLegacyPackedScanner(bytes.NewReader(out.Bytes()), deprecated.LegacyPackedScannerOpts{})
		for _, expected := range tc.in {
			expect.EQ(t, read(sc), string(expected))
		}
		expect.EQ(t, read(sc), "eof")
		sc.Reset(bytes.NewReader(out.Bytes()))
		for _, expected := range tc.in[:1] {
			expect.EQ(t, read(sc), string(expected))
		}
		sc.Reset(bytes.NewReader(out.Bytes()))
		for _, expected := range tc.in {
			expect.EQ(t, read(sc), string(expected))
		}
		expect.EQ(t, read(sc), "eof")
	}
}

func TestPackedMarshal(t *testing.T) {
	type js struct {
		I  int
		IS string
	}
	wropts := deprecated.LegacyPackedWriterOpts{
		MaxItems: 2,
		MaxBytes: 0,
	}
	wropts.Marshal = func(scratch []byte, v interface{}) ([]byte, error) {
		return json.Marshal(v)
	}
	scopts := deprecated.LegacyPackedScannerOpts{}
	scopts.Unmarshal = json.Unmarshal
	for i, tc := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 233} {
		out := &bytes.Buffer{}
		wr := deprecated.NewLegacyPackedWriter(out, wropts)
		for i := 0; i < tc; i++ {
			if _, err := wr.Marshal(&js{i, fmt.Sprintf("%d", i)}); err != nil {
				t.Fatalf("%v: %v", i, err)
			}
		}
		if err := wr.Flush(); err != nil {
			t.Fatalf("%v: %v", i, err)
		}
		sc := deprecated.NewLegacyPackedScanner(out, scopts)
		data := make([]js, tc)
		next := 0
		for sc.Scan() {
			if err := sc.Unmarshal(&data[next]); err != nil {
				t.Fatalf("%v: %v", next, err)
			}
			next++
		}
		if err := sc.Err(); err != nil {
			t.Fatalf("%v: %v", i, err)
		}
		for i, d := range data {
			w := &js{i, fmt.Sprintf("%v", i)}
			if got, want := &d, w; !reflect.DeepEqual(got, want) {
				t.Errorf("%v: got %v, want %v", i, got, want)
			}
		}
		if got, want := len(data), tc; got != want {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}
	}
}
func TestPackedMixed(t *testing.T) {
	type js struct {
		I  int
		IS string
	}
	indexedObjs := 0
	indexedBufs := 0
	wropts := deprecated.LegacyPackedWriterOpts{
		MaxItems: 2,
		MaxBytes: 0,
	}
	wropts.Marshal = func(scratch []byte, v interface{}) ([]byte, error) {
		return json.Marshal(v)
	}
	wropts.Index = func(record, recordSize, items uint64) (deprecated.ItemIndexFunc, error) {
		return func(offset, extent uint64, v interface{}, p []byte) error {
			if p != nil {
				indexedBufs++
			}
			if v != nil {
				indexedObjs++
			}
			return nil
		}, nil
	}
	scopts := deprecated.LegacyPackedScannerOpts{}
	scopts.Unmarshal = json.Unmarshal
	wantIndexedObjs, wantIndexedBufs := 0, 0
	for i, tc := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, 233} {
		out := &bytes.Buffer{}
		wr := deprecated.NewLegacyPackedWriter(out, wropts)
		for i := 0; i < tc; i++ {
			wantIndexedBufs++
			if i%2 == 0 {
				buf, err := json.Marshal(&js{i, fmt.Sprintf("%d", i)})
				if err != nil {
					t.Fatalf("%v: %v", i, err)
				}
				wr.Write(buf)
			} else {
				if _, err := wr.Marshal(&js{i, fmt.Sprintf("%d", i)}); err != nil {
					t.Fatalf("%v: %v", i, err)
				}
				wantIndexedObjs++
			}
		}
		if err := wr.Flush(); err != nil {
			t.Fatalf("%v: %v", i, err)
		}
		sc := deprecated.NewLegacyPackedScanner(out, scopts)
		data := make([]js, tc)
		next := 0
		for sc.Scan() {
			if err := sc.Unmarshal(&data[next]); err != nil {
				t.Fatalf("%v: %v", next, err)
			}
			next++
		}
		if err := sc.Err(); err != nil {
			t.Fatalf("%v: %v", i, err)
		}
		for i, d := range data {
			w := &js{i, fmt.Sprintf("%v", i)}
			if got, want := &d, w; !reflect.DeepEqual(got, want) {
				t.Errorf("%v: got %v, want %v", i, got, want)
			}
		}
		if got, want := len(data), tc; got != want {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}
	}

	if got, want := indexedBufs, wantIndexedBufs; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := indexedObjs, wantIndexedObjs; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPackedMax(t *testing.T) {
	max := deprecated.MaxPackedItems
	deprecated.MaxPackedItems = 2
	defer func() {
		deprecated.MaxPackedItems = max
	}()
	out := &bytes.Buffer{}
	wropts := deprecated.LegacyPackedWriterOpts{
		MaxItems: 200,
		MaxBytes: 0,
	}
	wr := deprecated.NewLegacyPackedWriter(out, wropts)
	wr.Write([]byte("hello 1"))
	wr.Write([]byte("hello 2"))
	wr.Write([]byte("hello 3"))
	wr.Write([]byte("hello 4"))
	wr.Write([]byte("hello 5"))
	if err := wr.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, want := bytes.Count(out.Bytes(), internal.MagicPacked[:]), 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPackedErrors(t *testing.T) {
	wropts := deprecated.LegacyPackedWriterOpts{
		MaxItems: 2,
		MaxBytes: 10,
	}
	buf := &bytes.Buffer{}
	wr := deprecated.NewLegacyPackedWriter(buf, wropts)

	bigbuf := [50]byte{}
	_, err := wr.Write(bigbuf[:])
	expect.HasSubstr(t, err, "buffer is too large 50 > 10")

	wropts.Marshal = func(scratch []byte, v interface{}) ([]byte, error) {
		return json.Marshal(v)
	}
	wr = deprecated.NewLegacyPackedWriter(buf, wropts)
	_, err = wr.Marshal(bigbuf[:])
	expect.HasSubstr(t, err, "buffer is too large 70 > 10")

	writeError := func(offset int, short bool, msg string) {
		ew := &fakeWriter{errAt: offset, short: short}
		wropts := deprecated.LegacyPackedWriterOpts{MaxItems: 1, MaxBytes: 10}
		wr := deprecated.NewLegacyPackedWriter(ew, wropts)
		_, err := wr.Write([]byte("hello"))
		expect.NoError(t, err, "first write succeeds")
		_, err = wr.Write([]byte("hello"))
		expect.HasSubstr(t, err, msg)
	}
	writeError(0, false, "recordio: failed to write header")
	writeError(22, false, "recordio: failed to write record")
	writeError(25, true, "recordio: buffered write too short")

	wropts.Transform = func(bufs [][]byte) ([]byte, error) {
		return nil, fmt.Errorf("transform oops")
	}
	wr = deprecated.NewLegacyPackedWriter(buf, wropts)
	wr.Write([]byte("oh"))
	err = wr.Flush()
	expect.HasSubstr(t, err, "transform oops")

	buf.Reset()
	wr = deprecated.NewLegacyPackedWriter(buf, deprecated.LegacyPackedWriterOpts{})
	wr.Write([]byte(""))
	if err := wr.Flush(); err != nil {
		t.Fatal(err)
	}

	shortReadError := func(offset int, msg string) {
		f := buf.Bytes()
		tmp := make([]byte, len(f))
		copy(tmp, f)
		s := deprecated.NewLegacyPackedScanner(bytes.NewBuffer(tmp[:offset]),
			deprecated.LegacyPackedScannerOpts{})
		if s.Scan() {
			t.Errorf("expected false")
		}
		expect.HasSubstr(t, s.Err(), msg)
	}
	shortReadError(1, "unexpected EOF")
	shortReadError(19, "unexpected EOF")
	shortReadError(20, "short/long record")

	scopts := deprecated.LegacyPackedScannerOpts{}
	scopts.Transform = func(scratch, buf []byte) ([]byte, error) {
		return nil, fmt.Errorf("transform oops")
	}
	sc := deprecated.NewLegacyPackedScanner(bytes.NewBuffer(buf.Bytes()), scopts)
	if sc.Scan() {
		t.Fatal("expected false")
	}
	if sc.Scan() {
		t.Fatal("expect false")
	}
	expect.HasSubstr(t, sc.Err(), "transform oops")
}

func readAll(t *testing.T, buf *bytes.Buffer, opts deprecated.LegacyPackedScannerOpts) []string {
	sc := deprecated.NewLegacyPackedScanner(buf, opts)
	var read []string
	for sc.Scan() {
		read = append(read, string(sc.Bytes()))
	}
	assert.NoError(t, sc.Err())
	return read
}

func TestPackedTransform(t *testing.T) {
	// Prepend __ and append ++ to every record.
	wropts := deprecated.LegacyPackedWriterOpts{
		MaxItems: 2,
		Transform: func(bufs [][]byte) ([]byte, error) {
			r := []byte("__")
			for _, b := range bufs {
				r = append(r, b...)
			}
			return append(r, []byte("++")...), nil
		},
	}
	buf := &bytes.Buffer{}
	wr := deprecated.NewLegacyPackedWriter(buf, wropts)
	data := []string{"Hello", "World", "How", "Are", "You?"}
	for _, d := range data {
		wr.Write([]byte(d))
	}
	wr.Flush()

	// Scan with the __ and ++ in place. Each item in the each
	// record is the same original size, but the contents are
	// 'shifted' by the leading__
	expected := []string{"__Hel", "loWor", "__H", "owA", "__Yo"}
	saved := make([]byte, len(buf.Bytes()))
	copy(saved, buf.Bytes())

	read := readAll(t, buf, deprecated.LegacyPackedScannerOpts{})
	if got, want := len(expected), len(expected); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	for i, r := range read {
		if got, want := r, expected[i]; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
	}

	scopts := deprecated.LegacyPackedScannerOpts{}
	// Strip the leading ++ and trailing ++ while scanning
	scopts.Transform = func(scratch, buf []byte) ([]byte, error) {
		return buf[2 : len(buf)-2], nil
	}
	buf = bytes.NewBuffer(saved)
	read = readAll(t, buf, scopts)
	if got, want := len(read), len(data); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	for i, r := range read {
		if got, want := r, data[i]; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
	}
}

func createBufs(nBufs, maxSize int) (map[string][]byte, error) {
	rand.Seed(time.Now().UnixNano())
	out := map[string][]byte{}
	for i := 0; i < nBufs; i++ {
		size := rand.Intn(maxSize)
		if size == 0 {
			size = maxSize / 2
		}
		buf := make([]byte, size)
		n, err := rand.Read(buf)
		if err != nil || n != cap(buf) {
			return nil, fmt.Errorf("failed to generate %d bytes of random data: %d != %d: %v", cap(buf), n, cap(buf), err)
		}
		s := sha256.Sum256(buf)
		k := hex.EncodeToString(s[:])
		if _, present := out[k]; present {
			// avoid dups.
			i--
			continue
		}
		out[k] = buf
	}
	return out, nil
}

func TestPackedConcurrentWrites(t *testing.T) {
	nbufs := 200
	maxBufSize := 1 * 1024 * 1024
	data, err := createBufs(nbufs, maxBufSize)
	assert.NoError(t, err)

	tmpdir, cleanup := testutil.TempDir(t, "", "encrypted-test")
	defer testutil.NoCleanupOnError(t, cleanup, "tmpdir: ", tmpdir)
	buf := &bytes.Buffer{}
	wr := deprecated.NewLegacyPackedWriter(buf, deprecated.LegacyPackedWriterOpts{MaxItems: 10})

	var wg sync.WaitGroup
	wg.Add(len(data))
	ch := make(chan error, nbufs)
	for _, v := range data {
		go func(b []byte) {
			_, err := wr.Write(b)
			ch <- err
			wg.Done()
		}(v)
	}

	wg.Wait()
	wr.Flush()
	close(ch)
	for err := range ch {
		assert.NoError(t, err)
	}

	scanner := deprecated.NewLegacyPackedScanner(buf, deprecated.LegacyPackedScannerOpts{})
	for scanner.Scan() {
		buf := scanner.Bytes()
		sum := sha256.Sum256(buf)
		key := hex.EncodeToString(sum[:])
		if _, present := data[key]; !present {
			t.Errorf("corrupt/wrong data %v is not a sha256 of one of the test bufs", key)
			continue
		}
		data[key] = nil
	}
	assert.NoError(t, scanner.Err())

	for k, v := range data {
		if v != nil {
			t.Errorf("failed to read buffer with sha256 of %v", k)
		}
	}
}
