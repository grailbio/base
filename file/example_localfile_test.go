package file_test

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/grailbio/base/file"
)

// Example_localfile is an example of basic read/write operations on the local
// file system.
func Example_localfile() {
	doWrite := func(ctx context.Context, data []byte, path string) {
		out, err := file.Create(ctx, path)
		if err != nil {
			panic(err)
		}
		if _, err = out.Writer(ctx).Write(data); err != nil {
			panic(err)
		}
		if err := out.Close(ctx); err != nil {
			panic(err)
		}
	}

	doRead := func(ctx context.Context, path string) []byte {
		in, err := file.Open(ctx, path)
		if err != nil {
			panic(err)
		}
		data, err := ioutil.ReadAll(in.Reader(ctx))
		if err != nil {
			panic(err)
		}
		if err := in.Close(ctx); err != nil {
			panic(err)
		}
		return data
	}

	ctx := context.Background()
	doWrite(ctx, []byte("Blue box jumped over red bat"), "/tmp/foohah.txt")
	fmt.Printf("Got: %s\n", string(doRead(ctx, "/tmp/foohah.txt")))
	// Output:
	// Got: Blue box jumped over red bat
}
