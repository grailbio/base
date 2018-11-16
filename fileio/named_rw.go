// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package fileio

import "io"

// Reader is io.Reader with an additional Name method that returns
// the name of the original source of the reader.
type Reader interface {
	io.Reader
	Name() string
}

// ReadCloser is io.ReadCloser with an additional Name method that returns
// the name of the original source of the reader.
type ReadCloser interface {
	io.ReadCloser
	Name() string
}

// Writer is io.Writer with an additional Name method that returns
// the name of the original source of the writer.
type Writer interface {
	io.Writer
	Name() string
}

// WriteCloser is io.WriteCloser with an additional Name method that returns
// the name of the original source of the writer.
type WriteCloser interface {
	io.WriteCloser
	Name() string
}

// ReadWriteCloser is an interface that implements io.ReadWriteCloser with an
// additional Name method that returns the name of the original source of the
// writer.
type ReadWriteCloser interface {
	io.ReadWriteCloser
	Name() string
}

// Closer is io.Closer with an additional Name method that returns
// the name of the original source of the closer.
type Closer interface {
	io.Closer
	Name() string
}
