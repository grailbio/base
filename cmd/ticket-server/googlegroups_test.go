// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"v.io/v23/security"
	"v.io/v23/security/access"
)

var (
	testDomainList = []string{"grailbio.com", "contractors.grail.com"}
)

func TestInit(t *testing.T) {
	f := func() {
		hostedDomains = nil
		googleGroupsInit("admin@grailbio.com")
	}
	assert.PanicsWithValue(t, "hostedDomains not initialized", f)

	f = func() {
		googleBlesserInit([]string{})
		googleGroupsInit("admin@grailbio.com")
	}
	assert.PanicsWithValue(t, "hostedDomains not initialized", f)
}

func TestEmail(t *testing.T) {
	googleBlesserInit(testDomainList)
	googleGroupsInit("admin@grailbio.com")

	cases := []struct {
		blessing string
		email    string
	}{
		{"v23.grail.com:google:razvanm@grailbio.com", "razvanm@grailbio.com"},
		{"v23.grail.com:google:razvanm@grailbio.com:_role", "razvanm@grailbio.com"},
		{"v23.grail.com:google:complex_+.email@grailbio.com:_role", "complex_+.email@grailbio.com"},
		{"v23.grail.com:google:razvanm@grailbioacom", ""},
		{"v23.grail.com:google:razvanm@gmail.com", ""},
		{"v23.grail.com:google:razvanm@", ""},
		{"v23.grail.com:google:razvanm", ""},
		{"v23.grail.com:google", ""},
		{"v23.grail.com:xxx:razvanm@grailbio.com", ""},
		{"v23.grail.com:googlegroups:eng@grailbio.com", ""},
		{"v23.grail.com:googlegroups:golang-nuts@googlegroups.com:google:razvanm@grailbio.com", ""},
		{"v23.grail.com:googlegroups:eng@grailbio.com:google:razvanm@grailbio.com", ""},
	}

	prefix := "v23.grail.com"
	for _, c := range cases {
		got, want := verifyAndExtractEmailFromBlessing(c.blessing, prefix), c.email
		if got != want {
			t.Errorf("email(%q, %q): got %q, want %q", c.blessing, prefix, got, want)
		}
	}
}

func TestGroup(t *testing.T) {
	googleBlesserInit(testDomainList)
	googleGroupsInit("admin@grailbio.com")

	cases := []struct {
		blessing string
		email    string
	}{
		{"v23.grail.com:googlegroups:eng-dev-access@grailbio.com", "eng-dev-access@grailbio.com"},
		{"v23.grail.com:googlegroups:golang-nuts@googlegroups.com", ""},
		{"v23.grail.com:googlegroups:golang-_+.nuts@grailbio.com", "golang-_+.nuts@grailbio.com"},
		{"v23.grail.com:googlegroups:eng@", ""},
		{"v23.grail.com:googlegroups:eng", ""},
		{"v23.grail.com:googlegroups", ""},
		{"v23.grail.com:xxx:eng@grailbio.com", ""},
		{"v23.grail.com:google:razvanm@grailbio.com", ""},
		{"v23.grail.com:google:razvanm@grailbio.com:googlegroups:golang-nuts@googlegroups.com", ""},
		{"v23.grail.com:google:razvanm@grailbio.com:googlegroups:eng@grailbio.com", ""},
	}

	prefix := "v23.grail.com"
	for _, c := range cases {
		got, want := extractGroupEmailFromBlessing(c.blessing, prefix), c.email
		if got != want {
			t.Errorf("email(%q, %q): got %q, want %q", c.blessing, prefix, got, want)
		}
	}
}

func TestAclIncludes(t *testing.T) {
	googleBlesserInit(testDomainList)
	googleGroupsInit("admin@grailbio.com")

	cases := []struct {
		acl  access.AccessList
		want bool
	}{
		{
			access.AccessList{
				In:    []security.BlessingPattern{},
				NotIn: []string{},
			},
			false,
		},
		{
			access.AccessList{
				In: []security.BlessingPattern{
					"v23.grail.com:google:razvanm@grailbio.com",
				},
				NotIn: []string{},
			},
			true,
		},
		{
			access.AccessList{
				In: []security.BlessingPattern{
					"v23.grail.com:googlegroups:eng-dev-access@grailbio.com",
				},
				NotIn: []string{},
			},
			true,
		},
		{
			access.AccessList{
				In: []security.BlessingPattern{},
				NotIn: []string{
					"v23.grail.com:googlegroups:eng-dev-access@grailbio.com",
				},
			},
			false,
		},
		{
			access.AccessList{
				In: []security.BlessingPattern{
					"v23.grail.com:google:razvanm@grailbio.com",
				},
				NotIn: []string{
					"v23.grail.com:googlegroups:eng-dev-access@grailbio.com",
				},
			},
			false,
		},
		{
			access.AccessList{
				In: []security.BlessingPattern{
					"v23.grail.com:googlegroups:eng-dev-access@grailbio.com",
				},
				NotIn: []string{
					"v23.grail.com:google:razvanm@grailbio.com",
				},
			},
			false,
		},
	}

	prefix := "v23.grail.com"
	blessings := []string{"v23.grail.com:google:razvanm@grailbio.com"}
	a := &authorizer{
		isMember: func(user, group string) bool {
			return user == "razvanm@grailbio.com" && group == "eng-dev-access@grailbio.com"
		},
	}
	for _, c := range cases {
		got := a.aclIncludes(c.acl, blessings, prefix)
		if got != c.want {
			t.Errorf("aclIncludes(%+v, %v): got %v, want %v", c.acl, blessings, got, c.want)
		}
	}
}
