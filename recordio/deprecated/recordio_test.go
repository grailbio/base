package deprecated_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/grailbio/base/recordio/deprecated"
	"github.com/grailbio/base/recordio/internal"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/expect"
	"github.com/klauspost/compress/gzip"
	"v.io/x/lib/gosh"
)

func cat(args ...[]byte) []byte {
	r := []byte{}
	for _, a := range args {
		r = append(r, a...)
	}
	return r
}

func cmp(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for ia, va := range a {
		if b[ia] != va {
			return false
		}
	}
	return true
}

func TestRecordioSimpleWriteRead(t *testing.T) {
	c := func(args []byte, m string) []byte {
		return cat(internal.MagicLegacyUnpacked[:], args, []byte(m))
	}
	for i, tc := range []struct {
		in  string
		out []byte
	}{
		{"", c([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0x69, 0xDF, 0x22, 0x65}, "")},
		{"a", c([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0xF7, 0xDF, 0x88, 0xA9}, "a")},
		{"hello\n", c([]byte{6, 0, 0, 0, 0, 0, 0, 0, 0xEE, 0xD6, 0x4D, 0xA3}, "hello\n")},
	} {
		out := &bytes.Buffer{}
		rw := deprecated.NewLegacyWriter(out, deprecated.LegacyWriterOpts{})
		n, err := rw.Write([]byte(tc.in))
		assert.NoError(t, err)
		assert.EQ(t, n, len(tc.in))
		if got, want := out.Bytes(), tc.out; !cmp(got, want) {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}

		s := deprecated.NewLegacyScanner(out, deprecated.LegacyScannerOpts{})
		assert.True(t, s.Scan())
		b := s.Bytes()
		if got, want := b, []byte(tc.in); !cmp(got, want) {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
		if got, want := s.Scan(), false; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
		if err := s.Err(); err != nil {
			t.Errorf("%d: %v", i, err)
		}
	}

	out := &bytes.Buffer{}
	rw := deprecated.NewLegacyWriter(out, deprecated.LegacyWriterOpts{})
	n, err := rw.WriteSlices([]byte("hello"), []byte(" "), []byte("world"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := n, 11; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	s := deprecated.NewLegacyScanner(out, deprecated.LegacyScannerOpts{})
	s.Scan()
	if got, want := s.Bytes(), []byte("hello world"); !bytes.Equal(got, want) {
		t.Errorf("got %s, want %s", got, want)
	}

	n, err = rw.WriteSlices(nil, []byte("hello"), []byte(" "), []byte("world"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := n, 11; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	s = deprecated.NewLegacyScanner(out, deprecated.LegacyScannerOpts{})
	s.Scan()
	if got, want := s.Bytes(), []byte("hello world"); !bytes.Equal(got, want) {
		t.Errorf("got %s, want %s", got, want)
	}
}

type fakeWriter struct {
	errAt int
	s     int
	short bool // set for a 'short writes'
}

func (fw *fakeWriter) Write(p []byte) (n int, err error) {
	fw.s += len(p)
	if fw.s > fw.errAt {
		if fw.short {
			return 0, nil
		}
		return 0, fmt.Errorf("error at %d", fw.errAt)
	}
	return len(p), nil
}

type fakeReader struct {
	buf   *bytes.Buffer
	errAt int
	s     int
}

func (fr *fakeReader) Read(p []byte) (n int, err error) {
	b := fr.buf.Next(len(p))
	fr.s += len(p)
	if fr.s > fr.errAt {
		return 0, fmt.Errorf("fail at %d", fr.errAt)
	}
	copy(p, b)
	return len(b), nil
}

func TestRecordioErrors(t *testing.T) {
	writeError := func(offset int, msg string) {
		ew := &fakeWriter{errAt: offset}
		rw := deprecated.NewLegacyWriter(ew, deprecated.LegacyWriterOpts{})
		_, err := rw.Write([]byte("hello"))
		expect.HasSubstr(t, err, msg)
	}
	writeError(0, "failed to write header")
	writeError(21, "failed to write record")

	marshalError := func(offset int, msg string) {
		ew := &fakeWriter{errAt: offset}
		rw := deprecated.NewLegacyWriter(ew, deprecated.LegacyWriterOpts{
			Marshal: recordioMarshal,
		})
		_, err := rw.Marshal(&TestPB{"oops"})
		expect.HasSubstr(t, err, msg)
	}
	marshalError(0, "failed to write header")
	marshalError(21, "failed to write record")

	buf := &bytes.Buffer{}
	rw := deprecated.NewLegacyWriter(buf, deprecated.LegacyWriterOpts{})
	rw.Write([]byte("hello\n"))

	corruptionError := func(offset int, msg string) {
		f := buf.Bytes()
		tmp := make([]byte, len(f))
		copy(tmp, f)
		tmp[offset] = 0xff
		s := deprecated.NewLegacyScanner(bytes.NewBuffer(tmp), deprecated.LegacyScannerOpts{})
		if s.Scan() {
			t.Errorf("expected false")
		}
		expect.HasSubstr(t, s.Err(), msg)
	}

	corruptionError(0, "invalid magic number")
	corruptionError(10, "crc check failed")
	corruptionError(17, "crc check failed")

	shortReadError := func(offset int, msg string) {
		f := buf.Bytes()
		tmp := make([]byte, len(f))
		copy(tmp, f)
		s := deprecated.NewLegacyScanner(bytes.NewBuffer(tmp[:offset]), deprecated.LegacyScannerOpts{})
		if s.Scan() {
			t.Errorf("expected false")
		}
		expect.HasSubstr(t, s.Err(), msg)
	}
	shortReadError(1, "unexpected EOF")
	shortReadError(19, "unexpected EOF")
	shortReadError(20, "short/long record")

	readError := func(offset int, msg string) {
		f := buf.Bytes()
		tmp := make([]byte, len(f))
		copy(tmp, f)
		rdr := &fakeReader{buf: bytes.NewBuffer(tmp), errAt: offset}
		s := deprecated.NewLegacyScanner(rdr, deprecated.LegacyScannerOpts{})
		if s.Scan() {
			t.Errorf("expected false")
		}
		expect.HasSubstr(t, s.Err(), msg)
	}
	readError(1, "failed to read header")
	readError(9, "failed to read header")
	readError(19, "failed to read header")
	readError(20, "failed to read record")

	defer func(oldSize uint64) {
		internal.MaxReadRecordSize = oldSize
	}(internal.MaxReadRecordSize)
	internal.MaxReadRecordSize = 100
	buf.Reset()
	rw = deprecated.NewLegacyWriter(buf, deprecated.LegacyWriterOpts{})
	rw.Write([]byte(strings.Repeat("a", 101)))
	s := deprecated.NewLegacyScanner(buf, deprecated.LegacyScannerOpts{})
	if got, want := s.Scan(), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	expect.HasSubstr(t, s.Err(), "unreasonably large read")

	ew := &fakeWriter{errAt: 25}
	wr := deprecated.NewLegacyWriter(ew, deprecated.LegacyWriterOpts{})
	_, err := wr.WriteSlices([]byte("hello"), []byte(" "), []byte("world"))
	expect.HasSubstr(t, err, "recordio: failed to write record")
}

func TestRecordioEmpty(t *testing.T) {
	out := &bytes.Buffer{}
	s := deprecated.NewLegacyScanner(out, deprecated.LegacyScannerOpts{})
	if got, want := s.Scan(), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if s.Bytes() != nil {
		t.Errorf("expected nil slice")
	}
	if got, want := s.Scan(), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if err := s.Err(); err != nil {
		t.Errorf("%v", err)
	}
}

func TestRecordioMultiple(t *testing.T) {
	out := &bytes.Buffer{}
	rw := deprecated.NewLegacyWriter(out, deprecated.LegacyWriterOpts{})
	expected := []string{"", "hello", "world", "", "last record"}
	for _, str := range expected {
		n, err := rw.Write([]byte(str))
		assert.NoError(t, err)
		assert.True(t, n == len(str))
	}

	read := func(s deprecated.LegacyScanner) string {
		if !s.Scan() {
			expect.NoError(t, s.Err())
			return "eof"
		}
		return string(s.Bytes())
	}
	s := deprecated.NewLegacyScanner(bytes.NewReader(out.Bytes()), deprecated.LegacyScannerOpts{})
	for _, str := range expected {
		expect.EQ(t, read(s), str)
	}
	expect.EQ(t, "eof", read(s))

	s.Reset(bytes.NewReader(out.Bytes()))
	for _, str := range expected[:2] {
		expect.EQ(t, read(s), str)
	}
	s.Reset(bytes.NewReader(out.Bytes()))
	for _, str := range expected {
		expect.EQ(t, read(s), str)
	}
	expect.EQ(t, read(s), "eof")
}

type lazyReader struct {
	rd io.Reader
}

func (lz *lazyReader) Read(p []byte) (n int, err error) {
	lazy := len(p)
	if lazy > 10 {
		lazy -= 10
	}
	return io.ReadFull(lz.rd, p[:lazy])
}

func TestRecordioWriteRead(t *testing.T) {
	gosh := gosh.NewShell(t)
	f := gosh.MakeTempFile()
	rw := deprecated.NewLegacyWriter(f, deprecated.LegacyWriterOpts{})
	contents := []string{"hello", "world", "!"}
	for _, rec := range contents {
		rw.Write([]byte(rec))
	}
	name := f.Name()
	f.Close()

	o := gosh.Cmd("gzip", "--keep", name).CombinedOutput()
	if got, want := o, ""; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	readall := func(rd io.Reader) ([][]byte, error) {
		r := [][]byte{}
		s := deprecated.NewLegacyScanner(rd, deprecated.LegacyScannerOpts{})
		for s.Scan() {
			r = append(r, s.Bytes())
		}
		return r, s.Err()
	}

	for _, n := range []string{name} {
		rd, err := os.Open(n)
		if err != nil {
			t.Fatalf("%v: %v", n, err)
		}
		gz, err := os.Open(n + ".gz")
		if err != nil {
			t.Fatalf("%v: %v", n, err)
		}
		gzrd, err := gzip.NewReader(gz)
		if err != nil {
			t.Fatalf("%v: %v", n, err)
		}

		raw, err := readall(rd)
		if err != nil {
			t.Fatalf("%v (raw): read %d records, %v", n, len(raw), err)
		}

		compressed, err := readall(gzrd)
		if err != nil {
			t.Fatalf("%v (gzip): read %d records (%d raw), %v", n, len(compressed), len(raw), err)
		}

		buf, err := ioutil.ReadFile(n)
		if err != nil {
			t.Fatal(err)
		}
		lzr := &lazyReader{rd: bytes.NewBuffer(buf)}
		lazy, err := readall(lzr)
		if err != nil {
			t.Fatalf("%v (lazyreader): read %d records (%d raw), %v", n, len(lazy), len(raw), err)

		}
		if got, want := len(raw), len(compressed); got != want {
			t.Errorf("%v: got %v, want %v", n, got, want)
		}
		if got, want := raw, compressed; !reflect.DeepEqual(got, want) {
			t.Errorf("%v: got %v, want %v", n, got, want)
		}
		if got, want := raw, lazy; !reflect.DeepEqual(got, want) {
			t.Errorf("%v: got %v, want %v", n, got, want)
		}
	}
}
