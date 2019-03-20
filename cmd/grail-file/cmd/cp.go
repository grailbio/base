package cmd

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"strings"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/traverse"
)

func Cp(ctx context.Context, out io.Writer, args []string) error {
	var (
		flags         flag.FlagSet
		verboseFlag   = flags.Bool("v", false, "Enable verbose logging")
		recursiveFlag = flags.Bool("R", false, "Recursive copy")
	)
	if err := flags.Parse(args); err != nil {
		return err
	}
	args = flags.Args()

	// Copy a regular file. The first return value is true if the source exists as
	// a regular file.
	copyRegularFile := func(src, dst string) (bool, error) {
		if *verboseFlag {
			fmt.Fprintf(os.Stderr, "%s -> %s\n", src, dst) // nolint: errcheck
		}
		in, err := file.Open(ctx, src)
		if err != nil {
			return false, err
		}
		defer in.Close(ctx) // nolint: errcheck
		// If the file "src" doesn't exist, either Open or Stat should fail.
		if _, err := in.Stat(ctx); err != nil {
			return false, err
		}
		out, err := file.Create(ctx, dst)
		if err != nil {
			return true, errors.E(err, fmt.Sprintf("cp %v->%v", src, dst))
		}
		if _, err := io.Copy(out.Writer(ctx), in.Reader(ctx)); err != nil {
			_ = out.Close(ctx)
			return true, errors.E(err, fmt.Sprintf("cp %v->%v", src, dst))
		}
		err = out.Close(ctx)
		if err != nil {
			err = errors.E(err, fmt.Sprintf("cp %v->%v", src, dst))
		}
		return true, err
	}

	// Copy a regular file or a directory.
	copyFile := func(src, dst string) error {
		if srcExists, err := copyRegularFile(src, dst); srcExists || !*recursiveFlag {
			return err
		}
		return forEachFile(ctx, src, func(path string) error {
			suffix := path[len(src):]
			for len(suffix) > 0 && suffix[0] == '/' {
				suffix = suffix[1:]
			}
			_, e := copyRegularFile(file.Join(src, suffix), file.Join(dst, suffix))
			return e
		})
	}

	copyFileInDir := func(src, dstDir string) error {
		return copyFile(src, file.Join(dstDir, file.Base(src)))
	}

	nArg := len(args)
	if nArg < 2 {
		return errors.New("Usage: cp src... dst")
	}
	dst := args[nArg-1]
	if _, hasGlob := parseGlob(dst); hasGlob {
		return fmt.Errorf("cp: destination %s cannot be a glob", dst)
	}
	srcs := expandGlobs(ctx, args[:nArg-1])
	if len(srcs) == 1 {
		// Try copying to dst. Failing that, copy to dst/<srcbasename>.
		if !strings.HasSuffix(dst, "/") && copyFile(srcs[0], dst) == nil {
			return nil
		}
		return copyFileInDir(srcs[0], dst)
	}
	return traverse.Limit(parallelism).Each(len(srcs), func(i int) error {
		return copyFileInDir(srcs[i], dst)
	})
}
