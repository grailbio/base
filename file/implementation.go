// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package file

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Implementation implements operations for a file-system type.
// Thread safe.
type Implementation interface {
	// String returns a diagnostic string.
	String() string

	// Open opens a file for reading. The pathname given to file.Open() is passed
	// here unchanged. Thus, it contains the URL prefix such as "s3://".
	//
	// Open returns an error of kind errors.NotExist if there is
	// no file at the provided path.
	Open(ctx context.Context, path string, opts ...Opts) (File, error)

	// Create opens a file for writing. If "path" already exists, the old contents
	// will be destroyed. If "path" does not exist already, the file will be newly
	// created.  If the directory part of the path does not exist already, it will
	// be created. The pathname given to file.Open() is passed here unchanged.
	// Thus, it contains the URL prefix such as "s3://".
	Create(ctx context.Context, path string, opts ...Opts) (File, error)

	// List finds files and directories. If "path" points to a regular file, the
	// lister will return information about the file itself and finishes.
	//
	// If "path" is a directory, the lister will list file and directory under the
	// given path.  When "recursive" is set to false, List finds files "one level"
	// below dir.  Dir may end in /, but need not.  All the files and directories
	// returned by the lister will have pathnames of the form dir/something.
	//
	// For key based storage engines (e.g. S3), a dir prefix not ending in "/" must
	// be followed immediately by "/" in some object keys, and only such keys
	// will be returned.
	// With "recursive=true" List finds all files whose pathnames under "dir" or its
	// subdirectories.  All the files returned by the lister will have pathnames of
	// the form dir/something.  Directories will not be returned as separate entities.
	// For example List(ctx, "foo",true) will yield "foo/bar/bat.txt", but not "foo.txt"
	// or "foo/bar/", while List(ctx, "foo", false) will yield "foo/bar", and
	// "foo/bat.txt", but not "foo.txt" or "foo/bar/bat.txt".  There is no difference
	// in the return value of List(ctx, "foo", ...) and List(ctx, "foo/", ...)
	List(ctx context.Context, path string, recursive bool) Lister

	// Stat returns the file metadata.  It returns nil if path is
	// a directory. (There is no direct test for existence of a
	// directory.)
	//
	// Stat returns an error of kind errors.NotExist if there is
	// no file at the provided path.
	Stat(ctx context.Context, path string, opts ...Opts) (Info, error)

	// Remove removes the file. The path passed to file.Remove() is passed here
	// unchanged.
	Remove(ctx context.Context, path string) error

	// Presign returns a URL that can be used to perform the given HTTP method,
	// usually one of "GET", "PUT" or "DELETE", on the path for the duration
	// specified in expiry.
	//
	// It returns an error of kind errors.NotSupported for implementations that
	// do not support signed URLs, or that do not support the given HTTP method.
	//
	// Unlike Open and Stat, this method does not return an error of kind
	// errors.NotExist if there is no file at the provided path.
	Presign(ctx context.Context, path, method string, expiry time.Duration) (url string, err error)
}

// Lister lists files in a directory tree. Not thread safe.
type Lister interface {
	// Scan advances the lister to the next entry.  It returns
	// false either when the scan stops because we have reached the end of the input
	// or else because there was error.  After Scan returns, the Err method returns
	// any error that occurred during scanning.
	Scan() bool

	// Err returns the first error that occurred while scanning.
	Err() error

	// Path returns the last path that was scanned. The path always starts with
	// the directory path given to the List method.
	//
	// REQUIRES: Last call to Scan returned true.
	Path() string

	// IsDir() returns true if Path() refers to a directory in a file system
	// or a common prefix ending in "/" in S3.
	//
	// REQUIRES: Last call to Scan returned true.
	IsDir() bool

	// Info returns metadata of the file that was scanned.
	//
	// REQUIRES: Last call to Scan returned true.
	Info() Info
}

type implementationFactory func() Implementation

var (
	mu                sync.RWMutex
	implFactories     = make(map[string]implementationFactory)
	impls             = make(map[string]Implementation)
	localImplInstance = NewLocalImplementation()
)

// RegisterImplementation arranges so that ParsePath(schema + "://anystring")
// will return (impl, "anystring", nil) in the future. Schema is a string such
// as "s3", "http".
//
// RegisterImplementation() should generally be called when the process starts.
// implFactory will be invoked exactly once, upon the first request to this scheme;
// this allows you to register with a factory that has not yet been full configured
// (e.g., it requires parsing command line flags) as long as it will be configured
// before the first request.
//
// REQUIRES: This function has not been called with the same schema before.
func RegisterImplementation(scheme string, implFactory func() Implementation) {
	if implFactory == nil {
		panic("Emptyl impl")
	}
	mu.Lock()
	defer mu.Unlock()
	if scheme == "" {
		panic("Empty scheme")
	}
	if _, ok := implFactories[scheme]; ok {
		panic(fmt.Sprintf("register %s: file scheme already registered", scheme))
	}
	implFactories[scheme] = implFactory
}

// FindImplementation returns an Implementation object registered for the given
// scheme.  It returns nil if the scheme is not registered.
func FindImplementation(scheme string) Implementation {
	if scheme == "" {
		return localImplInstance
	}
	mu.RLock()

	// First look for an existing implementation
	if impl, ok := impls[scheme]; ok {
		mu.RUnlock()
		return impl
	}

	// Next, look for a factory to make an implementation
	mu.RUnlock()
	mu.Lock()
	if implFactory, ok := implFactories[scheme]; ok {
		// Double check first that no one else created the implementation
		// while we upgraded to the write lock
		var impl Implementation
		if impl, ok = impls[scheme]; !ok {
			impl = implFactory()
			impls[scheme] = impl
		}
		mu.Unlock()
		return impl
	}

	// If neither of the above, then there's no implementation
	mu.Unlock()
	return nil
}

func findImpl(path string) (Implementation, error) {
	scheme, _, err := ParsePath(path)
	if err != nil {
		return nil, err
	}
	impl := FindImplementation(scheme)
	if impl == nil {
		return nil, fmt.Errorf("parsepath %s: no implementation registered for scheme %s", path, scheme)
	}
	return impl, nil
}

// Open opens the given file readonly.  It is a shortcut for calling
// ParsePath(), then FindImplementation, then Implementation.Open.
//
// Open returns an error of kind errors.NotExist if the file at the
// provided path does not exist.
func Open(ctx context.Context, path string, opts ...Opts) (File, error) {
	impl, err := findImpl(path)
	if err != nil {
		return nil, err
	}
	return impl.Open(ctx, path, opts...)
}

// Create opens the given file writeonly. It is a shortcut for calling
// ParsePath(), then FindImplementation, then Implementation.Create.
func Create(ctx context.Context, path string, opts ...Opts) (File, error) {
	impl, err := findImpl(path)
	if err != nil {
		return nil, err
	}
	return impl.Create(ctx, path, opts...)
}

// Stat returns the give file's metadata. Is a shortcut for calling ParsePath(),
// then FindImplementation, then Implementation.Stat.
//
// Stat returns an error of kind errors.NotExist if the file at the
// provided path does not exist.
func Stat(ctx context.Context, path string, opts ...Opts) (Info, error) {
	impl, err := findImpl(path)
	if err != nil {
		return nil, err
	}
	return impl.Stat(ctx, path, opts...)
}

type errorLister struct{ err error }

// Scan implements Lister.Scan.
func (e *errorLister) Scan() bool { return false }

// Path implements Lister.path.
func (e *errorLister) Path() string { panic("errorLister.Path" + e.err.Error()) }

// Info implements Lister.Info.
func (e *errorLister) Info() Info { panic("errorLister.Info" + e.err.Error()) }

// IsDir implements Lister.IsDir.
func (e *errorLister) IsDir() bool { panic("errorLister.IsDir" + e.err.Error()) }

// Err returns the Lister.Err.
func (e *errorLister) Err() error { return e.err }

// List finds all files whose pathnames under "dir" or its subdirectories.  All
// the files returned by the lister will have pathnames of form dir/something.
// For example List(ctx, "foo") will yield "foo/bar.txt", but not "foo.txt".
//
// Example: impl.List(ctx, "s3://grail-data/foo")
func List(ctx context.Context, prefix string, recursive bool) Lister {
	impl, err := findImpl(prefix)
	if err != nil {
		return &errorLister{err: err}
	}
	return impl.List(ctx, prefix, recursive)
}

// Remove is a shortcut for calling ParsePath(), then calling
// Implementation.Remove method.
func Remove(ctx context.Context, path string) error {
	impl, err := findImpl(path)
	if err != nil {
		return err
	}
	return impl.Remove(ctx, path)
}

// Presign is a shortcut for calling ParsePath(), then calling
// Implementation.Presign method.
func Presign(ctx context.Context, path, method string, expiry time.Duration) (string, error) {
	impl, err := findImpl(path)
	if err != nil {
		return "", err
	}
	return impl.Presign(ctx, path, method, expiry)
}

// Opts controls the file access requests, such as Open and Stat.
type Opts struct {
	// When set, this flag causes the file package to keep retrying when the file
	// is reported as not found. This flag should be set when:
	//
	// 1. you are accessing a file on S3, and
	//
	// 2. an application may have attempted to GET the same file in recent past
	// (~5 minutes). The said application may be on a different machine.
	//
	// This flag is honored only by S3 to work around the problem where s3 may
	// report spurious KeyNotFound error after a GET request to the same file.
	// For more details, see
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#CoreConcepts,
	// section "S3 Data Consistency Model". In particular:
	//
	//   The caveat is that if you make a HEAD or GET request to the key
	//   name (to find if the object exists) before creating the object, Amazon S3
	//   provides eventual consistency for read-after-write.
	RetryWhenNotFound bool

	// When set, Close will ignore NoSuchUpload error from S3
	// CompleteMultiPartUpload and silently returns OK.
	//
	// This is to work around a bug where concurrent uploads to one file sometimes
	// causes an upload request to be lost on the server side.
	// https://console.aws.amazon.com/support/cases?region=us-west-2#/6299905521/en
	// https://github.com/yasushi-saito/s3uploaderror
	//
	// Set this flag only if:
	//
	//  1. you are writing to a file on S3, and
	//
	//  2. possible concurrent writes to the same file produce the same
	//  contents, so you are ok with taking any of them.
	//
	// If you don't set this flag, then concurrent writes to the same file may
	// fail with a NoSuchUpload error, and it is up to you to retry.
	//
	// On non-S3 file systems, this flag is ignored.
	IgnoreNoSuchUpload bool
}
