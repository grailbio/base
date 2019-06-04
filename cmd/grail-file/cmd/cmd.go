package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/gobwas/glob"
	"github.com/gobwas/glob/syntax"
	"github.com/gobwas/glob/syntax/ast"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
)

var commands = []struct {
	name     string
	callback func(ctx context.Context, out io.Writer, args []string) error
	help     string
}{
	{"cat", Cat, `Cat prints contents of the files to the stdout. It supports globs defined in https://github.com/gobwas/glob.`},
	{"put", Put, `Put stores stdin to the provided path.`},
	{"ls", Ls, `List files`},
	{"rm", Rm, `Rm removes files. It supports globs defined in https://github.com/gobwas/glob.`},
	{"cp", Cp, `Cp copies files. It can be invoked in three forms:

1. cp src dst
2. cp src dst/
3. cp src.... dstdir

The first form first tries to copy file src to dst. If dst exists as a
directory, it copies src to dst/<base>, where <base> is the basename of the
source file.

The second form copies file src to dst/<base>.

The third form copies each of "src" to destdir/<base>.

This command supports globs defined in https://github.com/gobwas/glob.`},
}

func PrintHelp() {
	fmt.Fprintln(os.Stderr, "Subcommands:")
	for _, c := range commands {
		fmt.Fprintf(os.Stderr, "%s: %s\n", c.name, c.help)
	}
}

func Run(ctx context.Context, args []string) error {

	if len(args) == 0 {
		PrintHelp()
		return errors.E("No subcommand given")
	}
	for _, c := range commands {
		if c.name == args[0] {
			return c.callback(ctx, os.Stdout, args[1:])
		}
	}
	PrintHelp()
	return errors.E("unknown command", args[0])
}

const parallelism = 128

// forEachFile runs the callback for every file under the directory in
// parallel. It returns any of the errors returned by the callback.
func forEachFile(ctx context.Context, dir string, callback func(path string) error) error {
	err := errors.Once{}
	wg := sync.WaitGroup{}
	ch := make(chan string, parallelism*100)
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			for path := range ch {
				err.Set(callback(path))
			}
			wg.Done()
		}()
	}

	lister := file.List(ctx, dir, true /*recursive*/)
	for lister.Scan() {
		if !lister.IsDir() {
			ch <- lister.Path()
		}
	}
	close(ch)
	err.Set(lister.Err())
	wg.Wait()
	return err.Err()
}

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
	recursive := len(strings.Split(globSuffix, "/")) > 1 || strings.Contains(globSuffix, "**")

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
