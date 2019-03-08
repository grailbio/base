package cmd

import (
	"context"
	"io"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
)

func Cat(ctx context.Context, out io.Writer, args []string) (err error) {
	for _, arg := range expandGlobs(ctx, args) {
		f, err := file.Open(ctx, arg)
		if err != nil {
			return errors.E(err, "cat", arg)
		}
		defer file.CloseAndReport(ctx, f, &err)
		if _, err = io.Copy(out, f.Reader(ctx)); err != nil {
			return errors.E(err, "cat", arg)
		}
	}
	return nil
}
