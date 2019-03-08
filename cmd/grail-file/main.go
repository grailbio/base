package main

import (
	"context"
	"os"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/cmd/grail-file/cmd"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	err := cmd.Run(context.Background(), os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
}
