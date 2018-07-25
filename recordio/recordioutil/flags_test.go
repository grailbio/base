// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package recordioutil_test

import (
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/grailbio/base/recordio/recordioutil"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/expect"
	"github.com/klauspost/compress/flate"
)

func testCL(flags *recordioutil.WriterFlags, args ...string) error {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	recordioutil.RegisterWriterFlags(fs, flags)
	return fs.Parse(args)
}

func TestFlags(t *testing.T) {
	comp := func(level string, expected int) {
		flags := &recordioutil.WriterFlags{}
		expect.NoError(t, testCL(flags, "--recordio-compression-level="+level))
		expect.EQ(t, flags.CompressionFlag.Level, expected)
		expect.EQ(t, flags.CompressionFlag.String(), level)
		expect.EQ(t, flags.CompressionFlag.Specified, true)
	}
	for _, c := range []struct {
		cl string
		v  int
	}{
		{"none", flate.NoCompression},
		{"fastest", flate.BestSpeed},
		{"best", flate.BestCompression},
		{"default", flate.DefaultCompression},
		{"huffman-only", flate.HuffmanOnly},
	} {
		comp(c.cl, c.v)
	}

	kd := `{"registry":"reg", "keyid":"ff"}`
	tmpdir, cleanup := testutil.TempDir(t, "", "recorodioutil")
	defer cleanup()
	kf := filepath.Join(tmpdir, "kd")
	if err := ioutil.WriteFile(kf, []byte(kd), os.FileMode(0777)); err != nil {
		t.Fatal(err)
	}

	flags := &recordioutil.WriterFlags{}
	flags.ItemsPerRecord = 10
	if got, want := flags.ItemsPerRecord, uint(10); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	expect.NoError(t, testCL(flags, "--recordio-compression-level=best", "--recordio-MiB-per-record=33", "--recordio-items-per-record=66"))
	expect.EQ(t, flags.MegaBytesPerRecord, uint(33))
	expect.EQ(t, flags.ItemsPerRecord, uint(66))
	opts := recordioutil.WriterOptsFromFlags(flags)
	expected := recordioutil.WriterOpts{
		MaxItems:   66,
		MaxBytes:   33 * 1024 * 1024,
		FlateLevel: flate.BestCompression,
	}
	expect.EQ(t, opts, expected)
}

func TestFlagsErrors(t *testing.T) {
	flags := &recordioutil.WriterFlags{}
	err := testCL(flags, "--recordio-compression-level=x")
	expect.HasSubstr(t, err, "unrecognised compression option")

	defer func() {
		if r := recover(); r != nil {
			t.Logf("Recovered %v", r)
		} else {
			t.Fatal("failed to panic")
		}
	}()
	flags.CompressionFlag.Level = 33
	_ = flags.CompressionFlag.String()
}
