package cmd

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/grailbio/base/file"
	"github.com/grailbio/base/traverse"
)

func Rm(ctx context.Context, out io.Writer, args []string) error {
	var (
		flags         flag.FlagSet
		verboseFlag   = flags.Bool("v", false, "Enable verbose logging")
		recursiveFlag = flags.Bool("R", false, "Recursive remove")
	)
	if err := flags.Parse(args); err != nil {
		return err
	}
	args = expandGlobs(ctx, flags.Args())
	return traverse.Each(len(args), func(i int) error {
		path := args[i]
		if *verboseFlag {
			fmt.Fprintf(os.Stderr, "%s\n", path) // nolint: errcheck
		}
		if *recursiveFlag {
			return forEachFile(ctx, path, func(path string) error {
				if *verboseFlag {
					fmt.Fprintf(os.Stderr, "%s\n", path) // nolint: errcheck
				}
				return file.Remove(ctx, path)
			})
		}
		return file.Remove(ctx, path)
	})
}
