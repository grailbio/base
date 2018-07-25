// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package fileio_test

import (
	"testing"

	"github.com/grailbio/base/fileio"
)

func TestNames(t *testing.T) {
	if got, want := fileio.DetermineType("xx"), fileio.Other; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := fileio.DetermineType("xx.bar"), fileio.Other; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := fileio.DetermineType("xx.grail-rpk"), fileio.GrailRIOPacked; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := fileio.FileSuffix(fileio.GrailRIOPackedEncrypted), ".grail-rpk-kd"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := fileio.FileSuffix(fileio.Other), ""; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := fileio.DetermineAPI("xx.bar"), fileio.LocalAPI; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := fileio.DetermineAPI("s3://"), fileio.S3API; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestS3Spelling(t *testing.T) {
	for _, tc := range []struct {
		input     string
		api       fileio.StorageAPI
		corrected bool
		fixed     string
	}{
		{"s3://ok", fileio.S3API, false, "s3://ok"},
		{"s3://ok/a", fileio.S3API, false, "s3://ok/a"},
		{"s3://", fileio.S3API, false, "s3://"},
		{"s3:/", fileio.S3API, true, "s3://"},
		{"s3:/1", fileio.S3API, true, "s3://1"},
		{"s3:///2", fileio.S3API, true, "s3://2"},
		{"s3:///2/x", fileio.S3API, true, "s3://2/x"},
		{"s3:4", fileio.S3API, true, "s3://4"},
		{"s://5", fileio.S3API, true, "s3://5"},
		{"s:/6", fileio.S3API, true, "s3://6"},
		{"s33", fileio.LocalAPI, false, "s33"},
	} {
		api, corrected, fixed := fileio.SpellCorrectS3(tc.input)
		if got, want := api, tc.api; got != want {
			t.Errorf("%v: got %v, want %v", tc.input, got, want)
		}
		if got, want := corrected, tc.corrected; got != want {
			t.Errorf("%v: got %v, want %v", tc.input, got, want)
		}
		if got, want := fixed, tc.fixed; got != want {
			t.Errorf("%v: got %v, want %v", tc.input, got, want)
		}
	}
}
