package recordio_test

import (
	"bytes"
	"fmt"
	"io"

	"github.com/grailbio/base/recordio"
	"github.com/grailbio/base/recordio/recordioflate"
)

func init() {
	recordioflate.Init()
}

func doWrite(out io.Writer) {
	wr := recordio.NewWriter(out, recordio.WriterOpts{
		Transformers: []string{"flate"},
		Marshal:      func(scratch []byte, v interface{}) ([]byte, error) { return []byte(v.(string)), nil },
	})
	wr.Append("Item0")
	wr.Append("Item1")
	if err := wr.Finish(); err != nil {
		panic(err)
	}
}

func doRead(in io.ReadSeeker) {
	r := recordio.NewScanner(in, recordio.ScannerOpts{
		Unmarshal: func(data []byte) (interface{}, error) { return string(data), nil },
	})
	for r.Scan() {
		fmt.Printf("Item: %s\n", r.Get().(string))
	}
	if err := r.Err(); err != nil {
		panic(err)
	}
}

// Example_basic demonstrates basic reads, writes, and flate complession.
func Example_basic() {
	buf := &bytes.Buffer{}
	doWrite(buf)
	doRead(bytes.NewReader(buf.Bytes()))
	// Output:
	// Item: Item0
	// Item: Item1
}
