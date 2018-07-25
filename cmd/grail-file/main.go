package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gobwas/glob"
	"github.com/gobwas/glob/syntax"
	"github.com/gobwas/glob/syntax/ast"
	"github.com/grailbio/base/cmdutil"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/base/vcontext"
	"github.com/pkg/errors"
	"v.io/x/lib/cmdline"
)

// parseGlob parses a string that potentially contains glob metacharacters, and
// returns (nonglobprefix, hasglob). If the string does not contain any glob
// metacharacter, this function returns (str, false). Else, it returns the
// prefix of path elements up to the element containing a glob character.
//
// For example, parseGlob("foo/bar/baz*/*.txt" returns ("foo/bar", true).
func parseGlob(str string) (string, bool) {
	node, err := syntax.Parse(str)
	if err != nil {
		panic(err)
	}
	if node.Kind != ast.KindPattern || len(node.Children) == 0 {
		panic(node)
	}
	if node.Children[0].Kind != ast.KindText {
		return "", true
	}
	if len(node.Children) == 1 {
		return str, false
	}
	nonGlobPrefix := node.Children[0].Value.(ast.Text).Text
	if i := strings.LastIndexByte(nonGlobPrefix, '/'); i > 0 {
		nonGlobPrefix = nonGlobPrefix[:i+1]
	}
	return nonGlobPrefix, true
}

// expandGlob expands the given glob string. If the string does not contain a
// glob metacharacter, or on any error, it returns {str}.
func expandGlob(ctx context.Context, str string) []string {
	nonGlobPrefix, hasGlob := parseGlob(str)
	if !hasGlob {
		return []string{str}
	}
	m, err := glob.Compile(str)
	if err != nil {
		return []string{str}
	}

	globSuffix := str[len(nonGlobPrefix):]
	if strings.HasSuffix(globSuffix, "/") {
		globSuffix = globSuffix[:len(globSuffix)-1]
	}
	recursive := len(strings.Split(globSuffix, "/")) > 1 || strings.Index(globSuffix, "**") >= 0

	lister := file.List(ctx, nonGlobPrefix, recursive)
	matches := []string{}
	for lister.Scan() {
		if m.Match(lister.Path()) {
			matches = append(matches, lister.Path())
		}
	}
	if err := lister.Err(); err != nil {
		return []string{str}
	}
	if len(matches) == 0 {
		return []string{str}
	}
	return matches
}

// expandGlobs calls expandGlob on each string and unions the results.
func expandGlobs(ctx context.Context, patterns []string) []string {
	matches := []string{}
	for _, pattern := range patterns {
		matches = append(matches, expandGlob(ctx, pattern)...)
	}
	return matches
}

func runCat(_ *cmdline.Env, args []string) error {
	ctx := vcontext.Background()
	for _, arg := range expandGlobs(ctx, args) {
		f, err := file.Open(ctx, arg)
		if err != nil {
			return errors.Wrapf(err, "cat %v", arg)
		}
		defer f.Close(ctx) // nolint: errcheck
		if _, err = io.Copy(os.Stdout, f.Reader(ctx)); err != nil {
			return errors.Wrapf(err, "cat %v (io.Copy)", arg)
		}
	}
	return nil
}

func newCatCmd() *cmdline.Command {
	return &cmdline.Command{
		Runner:   cmdutil.RunnerFunc(runCat),
		Name:     "cat",
		Short:    "Print files to stdout",
		ArgsName: "files...",
		Long: `
This command prints contents of the files to the stdout. It supports globs defined in https://github.com/gobwas/glob.`,
	}
}

const parallelism = 16

type cprmOpts struct {
	verbose bool
}

func runRm(args []string, opts cprmOpts) error {
	ctx := vcontext.Background()
	args = expandGlobs(ctx, args)
	return traverse.Each(len(args)).Limit(parallelism).Do(func(i int) error {
		if opts.verbose {
			log.Printf("%s", args[i])
		}
		return file.Remove(ctx, args[i])
	})
}

func newRmCmd() *cmdline.Command {
	opts := cprmOpts{}
	c := &cmdline.Command{
		Runner:   cmdutil.RunnerFunc(func(_ *cmdline.Env, args []string) error { return runRm(args, opts) }),
		Name:     "rm",
		Short:    "Remove files",
		ArgsName: "files...",
		Long: `
This command removes files. It supports globs defined in https://github.com/gobwas/glob.`,
	}
	c.Flags.BoolVar(&opts.verbose, "v", false, "Enable verbose logging")
	return c
}

func runCp(args []string, opts cprmOpts) error {
	ctx := vcontext.Background()
	copyFile := func(src, dst string) error {
		if opts.verbose {
			log.Printf("%s -> %s", src, dst)
		}
		in, err := file.Open(ctx, src)
		if err != nil {
			return err
		}
		defer in.Close(ctx) // nolint: errcheck
		out, err := file.Create(ctx, dst)
		if err != nil {
			return errors.Wrapf(err, "cp %v->%v", src, dst)
		}
		if _, err := io.Copy(out.Writer(ctx), in.Reader(ctx)); err != nil {
			_ = out.Close(ctx)
			return errors.Wrapf(err, "cp %v->%v", src, dst)
		}
		err = out.Close(ctx)
		if err != nil {
			err = errors.Wrapf(err, "cp %v->%v", src, dst)
		}
		return err
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
	return traverse.Each(len(srcs)).Limit(parallelism).Do(func(i int) error {
		return copyFileInDir(srcs[i], dst)
	})
}

func newCpCmd() *cmdline.Command {
	opts := cprmOpts{}
	c := &cmdline.Command{
		Runner:   cmdutil.RunnerFunc(func(_ *cmdline.Env, args []string) error { return runCp(args, opts) }),
		Name:     "cp",
		Short:    "Copy files",
		ArgsName: "srcfiles... dstfile-or-dir",
		Long: `
This command copies files. It can be invoked in two forms:

1. cp src dst
2. cp src dst/
3. cp src.... dstdir

The first form first tries to copy file src to dst. If dst exists as a
directory, it copies src to dst/<base>, where <base> is the basename of the
source file.  The second form copies file src to dst/<base>.

The third form copies each of "src" to destdir/<base>.

This command supports globs defined in https://github.com/gobwas/glob.  `,
	}
	c.Flags.BoolVar(&opts.verbose, "v", false, "Enable verbose logging")
	return c
}

type lsOpts struct {
	recursive  bool
	longOutput bool
}

func runLs(out io.Writer, args []string, opts lsOpts) error {
	const iso8601 = "2006-01-02T15:04:05-0700"
	ctx := vcontext.Background()
	args = expandGlobs(ctx, args)
	for _, arg := range args {
		lister := file.List(ctx, arg, opts.recursive)
		for lister.Scan() {
			// TODO(saito) prettyprint
			switch {
			case lister.IsDir():
				fmt.Fprintf(out, "%s/\n", lister.Path())
			case opts.longOutput:
				info := lister.Info()
				fmt.Fprintf(out, "%s\t%d\t%s\n", lister.Path(), info.Size(), info.ModTime().Format(iso8601))
			default:
				fmt.Fprintf(out, "%s\n", lister.Path())
			}
		}
		if err := lister.Err(); err != nil {
			return err
		}
	}
	return nil
}

func newLsCmd() *cmdline.Command {
	opts := lsOpts{}
	c := &cmdline.Command{
		Runner:   cmdutil.RunnerFunc(func(_ *cmdline.Env, args []string) error { return runLs(os.Stdout, args, opts) }),
		Name:     "ls",
		Short:    "List files",
		ArgsName: "prefix...",
	}
	c.Flags.BoolVar(&opts.longOutput, "l", false, "Print file size and last modification time")
	c.Flags.BoolVar(&opts.recursive, "R", false, "Descend into directories recursively")
	return c
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	cmdline.HideGlobalFlagsExcept()
	cmd := &cmdline.Command{
		Name:  "grail-file",
		Short: "Access files using grailfile",
		Children: []*cmdline.Command{
			newCatCmd(),
			newCpCmd(),
			newLsCmd(),
			newRmCmd(),
		},
	}
	cmdline.Main(cmd)
}
