package recordio_test

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/grailbio/base/recordio"
)

type recordioIndex map[string]recordio.ItemLocation

func doWriteWithIndex(out io.Writer) {
	index := make(recordioIndex)
	wr := recordio.NewWriter(out, recordio.WriterOpts{
		Marshal: func(scratch []byte, v interface{}) ([]byte, error) { return []byte(v.(string)), nil },
		Index: func(loc recordio.ItemLocation, val interface{}) error {
			index[val.(string)] = loc
			return nil
		},
	})

	// To store a trailer block, AddHeader(recordio.KeyTrailer, true) must be
	// called beforehand.
	wr.AddHeader(recordio.KeyTrailer, true)
	wr.Append("Item0")
	wr.Append("Item1")
	wr.Append("Item2")
	wr.Flush()
	// Wait for the index callbacks to run.
	wr.Wait()

	// Write the index in the trailer.
	indexBuf := &bytes.Buffer{}
	encoder := gob.NewEncoder(indexBuf)
	if err := encoder.Encode(index); err != nil {
		panic(err)
	}
	wr.SetTrailer(indexBuf.Bytes())
	if err := wr.Finish(); err != nil {
		panic(err)
	}
}

func doReadWithIndex(in io.ReadSeeker) {
	r := recordio.NewScanner(in, recordio.ScannerOpts{
		Unmarshal: func(data []byte) (interface{}, error) { return string(data), nil },
	})
	// Read the trailer, parse it into the recordioIndex.
	decoder := gob.NewDecoder(bytes.NewReader(r.Trailer()))
	index := make(recordioIndex)
	if err := decoder.Decode(&index); err != nil {
		panic(err)
	}
	// Try reading individual items.
	r.Seek(index["Item1"])
	for r.Scan() {
		fmt.Printf("Item: %s\n", r.Get().(string))
	}
	r.Seek(index["Item0"])
	for r.Scan() {
		fmt.Printf("Item: %s\n", r.Get().(string))
	}
	if err := r.Err(); err != nil {
		panic(err)
	}
}

func Example_indexing() {
	buf := &bytes.Buffer{}
	doWriteWithIndex(buf)
	doReadWithIndex(bytes.NewReader(buf.Bytes()))
	// Output:
	// Item: Item1
	// Item: Item2
	// Item: Item0
	// Item: Item1
	// Item: Item2
}
