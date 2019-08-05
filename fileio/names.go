// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package fileio

import (
	"regexp"
	"strings"
)

// FileType represents the type of a file based on its filename
type FileType int

const (
	// Other represents a filetype other than the ones supported here.
	Other FileType = iota
	// Gzip file.
	Gzip
	// Bzip2 file.
	Bzip2
	// GrailRIO recordio.
	GrailRIO
	// GrailRIOPacked packed recordio.
	GrailRIOPacked
	// GrailRIOPackedEncrypted encrypted packed recordio.
	GrailRIOPackedEncrypted
	// GrailRIOPackedCompressed compressed packed recordio.
	GrailRIOPackedCompressed
	// GrailRIOPackedCompressedAndEncrypted compressed and encrypted packed recordio.
	GrailRIOPackedCompressedAndEncrypted
	// JSON text file
	JSON
	// Zstd format.
	// https://facebook.github.io/zstd/
	// https://tools.ietf.org/html/rfc8478
	Zstd
)

var lookup = map[string]FileType{
	".gz":              Gzip,
	".bz2":             Bzip2,
	".grail-rio":       GrailRIO,
	".grail-rpk":       GrailRIOPacked,
	".grail-rpk-kd":    GrailRIOPackedEncrypted,
	".grail-rpk-gz":    GrailRIOPackedCompressed,
	".grail-rpk-gz-kd": GrailRIOPackedCompressedAndEncrypted,
	".json":            JSON,
	".zst":             Zstd,
}

// StorageAPI represents the Storage API required to access a file.
type StorageAPI int

const (
	// LocalAPI represents a local fileystem accessible via a unix/posix API
	// and hence the io/os packages.
	LocalAPI StorageAPI = iota
	// S3API represents an Amazon S3 API.
	S3API
)

// DetermineAPI determines the Storage API that stores the file
// referred to by pathname.
func DetermineAPI(pathname string) StorageAPI {
	if strings.HasPrefix(pathname, "s3://") {
		return S3API
	}
	return LocalAPI
}

// DetermineType determines the type of the file given its filename.
func DetermineType(filename string) FileType {
	idx := strings.LastIndexByte(filename, '.')
	if idx < 0 {
		return Other
	}
	suffix := filename[idx:]
	return lookup[suffix]
}

// FileSuffix returns the filename suffix associated with the specified
// FileType.
func FileSuffix(typ FileType) string {
	for k, v := range lookup {
		if v == typ {
			return string(k)
		}
	}
	return ""
}

// IsGrailRecordio returns true if the filetype is one of the Grail recordio
// types.
func IsGrailRecordio(ft FileType) bool {
	switch ft {
	case GrailRIO, GrailRIOPacked,
		GrailRIOPackedEncrypted,
		GrailRIOPackedCompressed,
		GrailRIOPackedCompressedAndEncrypted:
		return true
	}
	return false
}

var (
	s3re0 = regexp.MustCompile("^s3://[^/]+.*$")
	s3re1 = regexp.MustCompile("^s3:/*(.*)$")
	s3re2 = regexp.MustCompile("^s:/+(.*)$")
	s3re3 = regexp.MustCompile("^s3/+(.*)$")
)

// SpellCorrectS3 returns true if the S3 path looks like an S3 path and returns
// the spell corrected path. That is, it returns true for common mispellings
// such as those show below along with the corrected s3://<path>
// s3:///<path>
// s3:<path>
// s3:/<path>
// s://<path>
// s:/<path>
// s3//<path>
func SpellCorrectS3(s3path string) (StorageAPI, bool, string) {
	if s3path == "s3://" || s3re0.MatchString(s3path) {
		return S3API, false, s3path
	}
	if strings.HasPrefix(s3path, "s3:") {
		fixed := s3re1.FindStringSubmatch(s3path)
		return S3API, true, "s3://" + fixed[1]
	}
	if strings.HasPrefix(s3path, "s:") {
		fixed := s3re2.FindStringSubmatch(s3path)
		return S3API, true, "s3://" + fixed[1]
	}
	if strings.HasPrefix(s3path, "s3/") {
		fixed := s3re3.FindStringSubmatch(s3path)
		return S3API, true, "s3://" + fixed[1]
	}
	return LocalAPI, false, s3path
}
