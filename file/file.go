// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package file

import (
	"context"
	"io"
)

// File defines operations on a file. Implementations must be thread safe.
type File interface {
	// String returns a diagnostic string.
	String() string

	// Name returns the path name given to file.Open or file.Create when this
	// object was created.
	Name() string

	// Stat returns file metadata.
	//
	// REQUIRES: Close has not been called
	Stat(ctx context.Context) (Info, error)

	// Reader creates an io.ReadSeeker object that operates on the file.  If
	// Reader() is called multiple times, they share the seek pointer.
	//
	// REQUIRES: Close has not been called
	Reader(ctx context.Context) io.ReadSeeker

	// Writer creates a writes that to the file. If Writer() is called multiple
	// times, they share the seek pointer.
	//
	// REQUIRES: Close has not been called
	Writer(ctx context.Context) io.Writer

	// Discard discards a file before it is closed, relinquishing any
	// temporary resources implied by pending writes. This should be
	// used if the caller decides not to complete writing the file.
	// Discard is a best-effort operation. Discard is not defined for
	// files opened for reading. Exactly one of Discard or Close should
	// be called. No other File, io.ReadSeeker, or io.Writer methods
	// shall be called after Discard.
	Discard(ctx context.Context) error

	// Closer commits the contents of a written file, invalidating the
	// File and all Readers and Writers created from the file. Exactly
	// one of Discard or Close should be called. No other File or
	// io.ReadSeeker, io.Writer methods shall be called after Close.
	Closer
}

// Closer cleans up a resource. Generally, resource provider implementations
// will return a Closer when opening a resource (like File above).
type Closer interface {
	// Close tries to clean up the resource. Implementations can define whether
	// Close can be called more than once and whether callers should retry on error.
	Close(context.Context) error
}

// NewErrorReader returns a new io.ReadSeeker object that returns "err" on any
// operation.
func NewErrorReader(err error) io.ReadSeeker { return &errorReaderWriter{err: err} }

// NewErrorWriter returns a new io.Writer object that returns "err" on any operation.
func NewErrorWriter(err error) io.Writer { return &errorReaderWriter{err: err} }

type errorReaderWriter struct{ err error }

func (r *errorReaderWriter) Read([]byte) (int, error) {
	return -1, r.err
}

func (r *errorReaderWriter) Seek(int64, int) (int64, error) {
	return -1, r.err
}

func (r *errorReaderWriter) Write([]byte) (int, error) {
	return -1, r.err
}
