package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/cmd/grail-fuse/gfs"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/s3file"
	"github.com/grailbio/base/log"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `Usage:
%s [flags...] MOUNTDIR

To unmount the file system, run "fusermount -u MOUNTDIR".
`, os.Args[0])
		flag.PrintDefaults()
	}
	remoteRootDirFlag := flag.String("remote-root-dir", "s3://", `Remote root directory`)
	logDirFlag := flag.String("log-dir", "", `Directory to store log files.
If empty, log messages are sent to stderr`)
	tmpDirFlag := flag.String("tmp-dir", "", `Tmp directory location. If empty, /tmp/gfscache-<uid> is used`)
	daemonFlag := flag.Bool("daemon", false, "Run in background")
	httpFlag := flag.String("http", "localhost:54321", "Run an HTTP status server")
	log.AddFlags()
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		log.Panic("fuse: missing mount point")
	}
	if len(*httpFlag) > 0 {
		log.Printf("starting status server at %s", *httpFlag)
		go func() {
			log.Print(http.ListenAndServe(*httpFlag, nil))
		}()
	}
	file.RegisterImplementation("s3", func() file.Implementation {
		return s3file.NewImplementation(s3file.NewDefaultProvider(session.Options{}), s3file.Options{})
	})
	gfs.Main(context.Background(), *remoteRootDirFlag, args[0], *daemonFlag, *tmpDirFlag, *logDirFlag)
}
