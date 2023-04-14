package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/grailbio/base/file/x/parlist"
	"github.com/grailbio/base/grail"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/base/vcontext"
	_ "grail.com/cloud/grailfile"
)

func main() {
	longOutput := flag.Bool("l", false, "Print file size and last modification time")
	recursive := flag.Bool("R", false, "Descend into directories recursively")
	parallelism := flag.Int("max-parallelism", 200, "Limit on concurrent S3 requests")
	log.AddFlags()

	cleanup := grail.Init()
	defer cleanup()

	ctx := vcontext.Background()

	if *parallelism <= 0 {
		*parallelism = 1
	}
	for _, arg := range flag.Args() {
		lister, err := parlist.List(ctx, arg, parlist.ListOpts{Recursive: *recursive})
		must.Nil(err)
		err = traverse.Limit(*parallelism).Each(*parallelism, func(int) (err error) {
			shard := lister.NewShard()
			if shard == nil {
				return nil
			}
			var out bytes.Buffer
			for more := true; more; {
				var batch []parlist.Info
				batch, more = shard.Scan(ctx)
				out.Reset()
				for _, info := range batch {
					var line string
					// Match grail-file ls's behavior.
					switch {
					case info.IsDir():
						line = info.Path() + "/"
					case *longOutput:
						line = fmtLong(info)
					default:
						line = info.Path()
					}
					_, err = fmt.Fprintln(&out, line)
					must.Nil(err)
				}
				_, err = os.Stdout.Write(out.Bytes())
				must.Nil(err)
			}
			return shard.Err()
		})
		must.Nil(err)
	}
}

// TODO(josh): Dedupe with grail-file/cmd/ls.go.
func fmtLong(info parlist.Info) string {
	const iso8601 = "2006-01-02T15:04:05-0700"
	return fmt.Sprintf("%s\t%d\t%s", info.Path(), info.Size(), info.ModTime().Format(iso8601))
}
