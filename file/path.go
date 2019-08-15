// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package file

import (
	"fmt"
	"path/filepath"
	"strings"
)

const (
	urlSeparator = '/'
)

// Compute the length of "foo" part of "foo://bar/baz". Returns (0,nil) if the
// path is for a local file system.
func getURLScheme(path string) (int, error) {
	// Scheme is always encoded in ASCII, per RFC3986.
	schemeLimit := -1
	for i := 0; i < len(path); i++ {
		ch := path[i]
		if ch == ':' {
			if len(path) <= i+2 || path[i+1] != '/' || path[i+2] != '/' {
				return -1, fmt.Errorf("parsepath %s: a URL must start with 'scheme://'", path)
			}
			schemeLimit = i
			break
		}
		if !((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '.' || ch == '+' || ch == '=') {
			break
		}
	}
	if schemeLimit == -1 {
		return 0, nil
	}
	return schemeLimit, nil
}

// ParsePath parses "path" and find the namespace object that can handle the
// path. The path can be of form either "scheme://path" just
// "path0/.../pathN". The latter indicates a local file.
//
// On success, "schema" will be the schema part of the path. "suffix" will be
// the path part after the scheme://. For example, ParsePath("s3://key/bucket")
// will return ("s3", "key/bucket", nil).
//
// For a local-filesystem path, this function returns ("", path, nil).
func ParsePath(path string) (scheme, suffix string, err error) {
	schemeLen, err := getURLScheme(path)
	if err != nil {
		return "", "", err
	}
	if schemeLen == 0 {
		return "", path, nil
	}
	return path[:schemeLen], path[schemeLen+3:], nil
}

// MustParsePath is similar to ParsePath, but crashes the process on error.
func MustParsePath(path string) (scheme, suffix string) {
	scheme, suffix, err := ParsePath(path)
	if err != nil {
		panic(err)
	}
	return scheme, suffix
}

// Base returns the last element of the path. It is the same as filepath.Base
// for a local filesystem path.  Else, it acts like filepath.Base, with the
// following differences: (1) the path separator is always '/'. (2) if the URL
// suffix is empty, it returns the path itself.
//
// Example:
//   file.Base("s3://") returns "s3://".
//   file.Base("s3://foo/hah/") returns "hah".
func Base(path string) string {
	scheme, suffix, err := ParsePath(path)
	if scheme == "" || err != nil {
		return filepath.Base(path)
	}
	if suffix == "" {
		// path is "s3://".
		return path
	}
	return filepath.Base(suffix)
}

// Dir returns the all but the last element of the path. It the same as
// filepath.Dir for a local filesystem path.  Else, it acts like filepath.Base,
// with the following differences: (1) the path separator is always '/'. (2) if
// the URL suffix is empty, it returns the path itself. (3) The path is not
// cleaned; for example repeated "/"s in the path is preserved.
func Dir(path string) string {
	scheme, suffix, err := ParsePath(path)
	if scheme == "" || err != nil {
		return filepath.Dir(path)
	}
	for i := len(suffix) - 1; i >= 0; i-- {
		if suffix[i] == urlSeparator {
			for i > 0 && suffix[i] == urlSeparator {
				i--
			}
			return path[:len(scheme)+3+i+1]
		}
	}
	return path[:len(scheme)+3]
}

// Join joins any number of path elements into a single path, adding a separator
// if necessary. It is the same as filepath.Join if elems[0] is a local
// filesystem path. Else, it works like filepath.Join, with the following
// differences: (1) the path separator is always '/'. (2) Each element is
// not cleaned; for example if an element contains repeated "/"s in the middle,
// they are preserved.
func Join(elems ...string) string {
	if len(elems) == 0 {
		return filepath.Join(elems...)
	}
	var prefix string
	n, err := getURLScheme(elems[0])
	if err == nil && n > 0 {
		prefix = elems[0][:n+3]
		elems[0] = elems[0][n+3:]
	} else if len(elems[0]) > 0 && elems[0][0] == '/' {
		prefix = "/"
		elems[0] = elems[0][1:]
	}

	// Remove leading (optional) or trailing "/"s from the string.
	clean := func(p string) string {
		var s, e int
		for s = 0; s < len(p); s++ {
			if p[s] != urlSeparator {
				break
			}
		}
		for e = len(p) - 1; e >= 0; e-- {
			if p[e] != urlSeparator {
				break
			}
		}
		if e < s {
			return ""
		}
		return p[s : e+1]
	}

	newElems := make([]string, 0, len(elems))
	for i := 0; i < len(elems); i++ {
		e := clean(elems[i])
		if e != "" {
			newElems = append(newElems, e)
		}
	}
	return prefix + strings.Join(newElems, "/")
}

// IsAbs returns true if pathname is absolute local path. For non-local file, it
// always returns true.
func IsAbs(path string) bool {
	if scheme, _, err := ParsePath(path); scheme == "" || err != nil {
		return filepath.IsAbs(path)
	}
	return true
}
