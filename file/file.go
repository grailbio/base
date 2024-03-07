// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package file

import (
	"context"
	"fmt"
	"io"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/ioctx"
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
	// For emphasis: these share state, which is different from OffsetReader!
	//
	// REQUIRES: Close has not been called
	Reader(ctx context.Context) io.ReadSeeker

	// OffsetReader creates a new, independent ioctx.ReadCloser, starting at
	// offset. Unlike Reader, its position in the file is only modified by Read
	// on this object. The returned object is not thread-safe, and callers are
	// responsible for serializing all of their calls, including calling Close
	// after all Reads are done. Of course, callers can use separate
	// OffsetReaders in parallel.
	//
	// Background: This API reflects S3's performance characteristics, where
	// initiating a new read position is relatively expensive, but then
	// streaming data is fast (including in parallel with multiple readers).
	//
	// REQUIRES: Close has not been called
	OffsetReader(offset int64) ioctx.ReadCloser

	// Writer creates a writes that to the file. If Writer() is called multiple
	// times, they share the seek pointer.
	//
	// REQUIRES: Close has not been called
	Writer(ctx context.Context) io.Writer

	// TODO: Introduce WriterAt() ioctx.WriterAt, analogous to ReaderAt.

	// Discard discards a file before it is closed, relinquishing any
	// temporary resources implied by pending writes. This should be
	// used if the caller decides not to complete writing the file.
	// Discard is a best-effort operation. Discard is not defined for
	// files opened for reading. Exactly one of Discard or Close should
	// be called. No other File, io.ReadSeeker, or io.Writer methods
	// shall be called after Discard.
	Discard(ctx context.Context)

	// Closer commits the contents of a written file, invalidating the
	// File and all Readers and Writers created from the file. Exactly
	// one of Discard or Close should be called. No other File or
	// io.ReadSeeker, io.Writer methods shall be called after Close.
	Closer
}

// TODO: Migrate callers to use new location.
type Closer = ioctx.Closer

// ETagged defines a getter for a file with an ETag.
type ETagged interface {
	// ETag is an identifier assigned to a specific version of the file.
	ETag() string
}

// CloseAndReport returns a defer-able helper that calls f.Close and reports errors, if any,
// to *err. Pass your function's named return error. Example usage:
//
//   func processFile(filename string) (_ int, err error) {
//     ctx := context.Background()
//     f, err := file.Open(ctx, filename)
//     if err != nil { ... }
//     defer file.CloseAndReport(ctx, f, &err)
//     ...
//   }
//
// If your function returns with an error, any f.Close error will be chained appropriately.
//
// Deprecated: Use errors.CleanUpCtx directly.
func CloseAndReport(ctx context.Context, f Closer, err *error) {
	errors.CleanUpCtx(ctx, f.Close, err)
}

// MustClose is a defer-able function that calls f.Close and panics on error.
//
// Example:
//   ctx := context.Background()
//   f, err := file.Open(ctx, filename)
//   if err != nil { panic(err) }
//   defer file.MustClose(ctx, f)
//   ...
func MustClose(ctx context.Context, f Closer) {
	if err := f.Close(ctx); err != nil {
		if n, ok := f.(named); ok {
			panic(fmt.Sprintf("close %s: %v", n.Name(), err))
		}
		panic(err)
	}
}

type named interface {
	// Name returns the path name given to file.Open or file.Create when this
	// object was created.
	Name() string
}

// Error implements io.{Reader,Writer,Seeker,Closer}. It returns the given error
// to any call.
type Error struct{ err error }

// NewError returns a new Error object that returns the given error to any
// Read/Write/Seek/Close call.
func NewError(err error) *Error { return &Error{err: err} }

// Read implements io.Reader
func (r *Error) Read([]byte) (int, error) {
	return -1, r.err
}

// Seek implements io.Seeker.
func (r *Error) Seek(int64, int) (int64, error) {
	return -1, r.err
}

// Write implements io.Writer.
func (r *Error) Write([]byte) (int, error) {
	return -1, r.err
}

// Close implements io.Closer.
func (r *Error) Close() error {
	return r.err
}
