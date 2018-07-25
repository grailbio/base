// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package file provides basic file operations across multiple file-system
// types. It is designed for use in applications that operate uniformly on
// multiple storage types, such as local files, S3 and HTTP.
//
// Overview
//
// This package is designed with following goals:
//
// - Support popular file systems, especially S3 and the local file system.
//
// - Define operation semantics that are implementable on all the supported file
// systems, yet practical and usable.
//
// - Extensible. Provide leeway to do things like registering new file system
// types or ticket-based authorizations.
//
// This package defines two key interfaces, Implementation and File.
//
// - Implementation provides filesystem operations, such as Open, Remove, and List
// (directory walking).
//
// - File implements operations on a file. It is created by
// Implementation.{Open,Create} calls. File is similar to go's os.File object
// but provides limited functionality.
//
// Reading and writing files
//
// The following snippet shows registering an S3 implementation, then writing
// and reading a S3 file.
//
//   import (
//    "context"
//    "ioutil"
//
//    "github.com/grailbio/base/file"
//    "github.com/grailbio/base/file/s3file"    // file.Implementation implementation for S3
//    "github.com/aws/aws-sdk-go/aws/session"
//    "github.com/stretchr/testify/require"
//   )
//
//   func init() {
//     file.RegisterImplementation("s3", s3file.NewImplementation(
//       s3file.NewDefaultProvider(session.Options{})))
//   }
//
//   // Caution: this code ignores all errors.
//   func WriteTest() {
//     ctx := context.Background()
//     f, err := file.Create(ctx, "s3://grail-saito/tmp/test.txt")
//     n, err = f.Writer(ctx).Write([]byte{"Hello"})
//     err = f.Close(ctx)
//   }
//
//   func ReadTest() {
//     ctx := context.Background()
//     f, err := file.Open(ctx, "s3://grail-saito/tmp/test.txt")
//     data, err := ioutil.ReadAll(f.Reader(ctx))
//     err = f.Close(ctx)
//   }
//
// To open a file for reading or writing, run file.Open("s3://bucket/key") or
// file.Create("s3://bucket/key"). A File object does not implement an io.Reader
// or io.Writer directly. Instead, you must call File.Reader or File.Writer to
// start reading or writing.  These methods are split from the File itself so
// that an application can pass different contexts to different I/O operations.
//
// File-system operations
//
// The file package provides functions similar to those in the standard os
// class.  For example, file.Remove("s3://bucket/key") removes a file, and
// file.Stat("s3://bucket/key") provides a metadata about the file.
//
// Pathname utility functions
//
// The file package also provides functions that are similar to those in the
// standard filepath package. Functions file.Base, file.Dir, file.Join work just
// like filepath.{Base,Dir,Join}, except that they handle the URL pathnames
// properly.  For example, file.Join("s3://foo", "bar") will return
// "s3://foo/bar", whereas filepath.Join("s3://foo", "bar") would return
// "s3:/foo/bar".
//
// Registering a filesystem implementation
//
// Function RegisterImplementation associates an implementation to a scheme
// ("s3", "http", "git", etc). A local file system implementation is
// automatically available without any explicit
// registration. RegisterImplementation is usually invoked when a process starts
// up, for all the supported file system types.  For example:
//
//   import (
//    "ioutil"
//    "github.com/grailbio/base/context"
//    "github.com/grailbio/base/file"
//    "github.com/grailbio/base/file/s3file"    // file.Implementation implementation for S3
//   )
//   func init() {
//     file.RegisterImplementation("s3:", s3file.NewImplementation(...))
//   }
//   func main() {
//     ctx := context.Background()
//     f, err := file.Open(ctx, "s3://somebucket/foo.txt")
//     data, err := ioutil.ReadAll(f.Reader(ctx))
//     err := f.Close(ctx)
//     ...
//   }
//
// Once an implementation is registered, the files for that scheme can be opened
// or created using "scheme:name" pathname.
//
// Differences from the os package
//
// The file package is similar to Go's standard os package.  The differences are
// the following.
//
// - The file package focuses on providing a file-like API for object storage
// systems, such as S3 or GCS.
//
// - Mutations to a File are restricted to whole-file writes. There is no option
// to overwrite a part of an existing file.
//
// - All the operations take a context parameter.
//
// - file.File does not implement io.Reader nor io.Writer directly. One must
// call File.Reader or File.Writer methods to obtains a reader or writer object.
//
// - Directories are simulated in a best-effort manner on implementations that do
// not support directories as first-class entities, such as S3.  Lister provides
// IsDir() for the current path.  Info(path) returns nil for directories.
//
// Concurrency
//
// The Implementation and File provide an open-close consistency.  More
// specifically, this package linearizes fileops, with a fileop defined in the
// following way: fileop is a set of operations, starting from
// Implementation.{Open,Create}, followed by read/write/stat operations on the
// file, followed by File.Close.  Operations such as
// Implementation.{Stat,Remove,List} and Lister.Scan form a singleton fileop.
//
// Caution: a local file system on NFS (w/o cache leasing) doesn't provide this
// guarantee.  Use NFS at your own risk.
package file
