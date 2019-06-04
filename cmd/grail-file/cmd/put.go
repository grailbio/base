package cmd

import (
	"context"
	"io"
	"os"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
)

func Put(ctx context.Context, out io.Writer, args []string) (err error) {
	if len(args) != 1 {
		return errors.New("put requires a single path")
	}
	arg := args[0]
	f, err := file.Create(ctx, arg)
	if err != nil {
		return errors.E(err, "put", arg)
	}
	defer file.CloseAndReport(ctx, f, &err)
	if _, err = io.Copy(f.Writer(ctx), os.Stdin); err != nil {
		return errors.E(err, "put", arg)
	}
	return nil
}
