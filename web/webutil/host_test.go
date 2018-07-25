// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package webutil_test

import (
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/grailbio/base/web/webutil"
)

func testPort(t *testing.T, protocol, defaultDomain, defaultPort string) {
	expectedDefaultDomain := strings.TrimPrefix(defaultDomain, ".")
	ad := func(a string) string {
		if len(expectedDefaultDomain) > 0 {
			return a + "." + expectedDefaultDomain
		}
		return a
	}

	ap := func(a string) string {
		if len(defaultPort) > 0 {
			return net.JoinHostPort(a, defaultPort)
		}
		return a
	}

	_, file, line, _ := runtime.Caller(1)
	loc := fmt.Sprintf("%s:%d", filepath.Base(file), line)
	for i, c := range []struct {
		srv, fqhp, fqh, hst, dmn, prt string
		err                           bool
	}{
		{"2001:db8::68", ap("2001:db8::68"), "2001:db8::68", "2001:db8::68", "", defaultPort, false},
		{"10.88.224.99", ap("10.88.224.99"), "10.88.224.99", "10.88.224.99", "", defaultPort, false},
		{"www", ap(ad("www")), ad("www"), "www", expectedDefaultDomain, defaultPort, false},
		{"www.b", ap("www.b"), "www.b", "www", "b", defaultPort, false},
		{"2001:db8::68[[", "", "", "", "", "", true},
	} {
		srv := protocol + c.srv
		ptcl, fqhp, fqh, hst, dmn, prt, err := webutil.CanonicalizeHost(srv, defaultDomain, defaultPort)
		testcase := fmt.Sprintf("%s:%d (%q, %q, %q:", loc, i, srv, defaultDomain, defaultPort)
		if got, want := c.err, (err != nil); got != want {
			t.Errorf("%s: got %v, want %v", testcase, got, want)
		}
		if got, want := ptcl, strings.TrimSuffix(protocol, "//"); got != want {
			t.Errorf("%s: got %v, want %v", testcase, got, want)
		}
		if got, want := fqhp, c.fqhp; got != want {
			t.Errorf("%s: got %v, want %v", testcase, got, want)
		}
		if got, want := fqh, c.fqh; got != want {
			t.Errorf("%s: got %v, want %v", testcase, got, want)
		}
		if got, want := hst, c.hst; got != want {
			t.Errorf("%s: got %v, want %v", testcase, got, want)
		}
		if got, want := dmn, c.dmn; got != want {
			t.Errorf("%s: got %v, want %v", testcase, got, want)
		}
		if got, want := prt, c.prt; got != want {
			t.Errorf("%s: got %v, want %v", testcase, got, want)
		}
	}
}

func TestCanonicalizeHost(t *testing.T) {
	for _, protocol := range []string{"", "https://", "http://", "foo://", "//"} {
		for _, defaultDomain := range []string{"", "x", "x.com", ".", ".x.com"} {
			for _, defaultPort := range []string{"", ":22", "81"} {
				testPort(t, protocol, defaultDomain, defaultPort)
			}
		}
	}
}
