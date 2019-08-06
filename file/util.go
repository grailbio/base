// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package file

import (
	"context"
	"fmt"
	"io/ioutil"

	"golang.org/x/sync/errgroup"
)

// ReadFile reads the given file and returns the contents. A successful call
// returns err == nil, not err == EOF. Arg opts is passed to file.Open.
func ReadFile(ctx context.Context, path string, opts ...Opts) ([]byte, error) {
	in, err := Open(ctx, path, opts...)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(in.Reader(ctx))
	if err != nil {
		in.Close(ctx) // nolint: errcheck
		return nil, err
	}
	return data, in.Close(ctx)
}

// WriteFile writes data to the given file. If the file does not exist,
// WriteFile creates it; otherwise WriteFile truncates it before writing.
func WriteFile(ctx context.Context, path string, data []byte) error {
	out, err := Create(ctx, path)
	if err != nil {
		return err
	}
	n, err := out.Writer(ctx).Write(data)
	if n != len(data) && err == nil {
		err = fmt.Errorf("writefile %s: requested to write %d bytes, actually wrote %d bytes", path, len(data), n)
	}
	if err != nil {
		out.Close(ctx) // nolint: errcheck
		return err
	}
	return out.Close(ctx)
}

// RemoveAll removes path and any children it contains.  It is unspecified
// whether empty directories are removed by this function.  It removes
// everything it can but returns the first error it encounters. If the path does
// not exist, RemoveAll returns nil.
func RemoveAll(ctx context.Context, path string) error {
	g, ectx := errgroup.WithContext(ctx)
	l := List(ectx, path, true)
	for l.Scan() {
		if !l.IsDir() {
			path := l.Path()
			g.Go(func() error { return Remove(ectx, path) })
		}
	}
	return g.Wait()
}
