package cmd

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/grailbio/base/file"
)

func Ls(ctx context.Context, out io.Writer, args []string) error {
	var (
		flags          flag.FlagSet
		longOutputFlag = flags.Bool("l", false, "Print file size and last modification time")
		recursiveFlag  = flags.Bool("R", false, "Descend into directories recursively")
	)
	if err := flags.Parse(args); err != nil {
		return err
	}
	type result struct {
		err   error
		lines chan string // stream of entries found for an arg, closed when done
	}
	longOutput := func(path string, info file.Info) string {
		// TODO(saito) prettyprint
		const iso8601 = "2006-01-02T15:04:05-0700"
		return fmt.Sprintf("%s\t%d\t%s", path, info.Size(), info.ModTime().Format(iso8601))
	}
	args = expandGlobs(ctx, flags.Args())
	results := make([]result, len(args))
	for i := range args {
		results[i].lines = make(chan string, 10000)
		go func(path string, r *result) {
			defer close(r.lines)
			// Check if the file is a regular file
			if info, err := file.Stat(ctx, path); err == nil {
				if *longOutputFlag {
					r.lines <- longOutput(path, info)
				} else {
					r.lines <- path
				}
				return
			}
			lister := file.List(ctx, path, *recursiveFlag)
			for lister.Scan() {
				switch {
				case lister.IsDir():
					r.lines <- lister.Path() + "/"
				case *longOutputFlag:
					r.lines <- longOutput(lister.Path(), lister.Info())
				default:
					r.lines <- lister.Path()
				}
			}
			r.err = lister.Err()
		}(args[i], &results[i])
	}
	// Print the results in order.
	var err error
	for i := range results {
		for line := range results[i].lines {
			_, _ = fmt.Fprintln(out, line)
		}
		if err2 := results[i].err; err2 != nil && err == nil {
			err = err2
		}
	}
	return err
}
