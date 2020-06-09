// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/grailbio/base/ttlcache"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/jwt"
	admin "google.golang.org/api/admin/directory/v1"
	v23context "v.io/v23/context"
	"v.io/v23/security"
	"v.io/v23/security/access"
	"v.io/v23/vdl"
	"v.io/x/lib/vlog"
)

type cacheKey struct {
	user  string
	group string
}

// cacheTTL is how long the entries in cache will be considered valid.
const cacheTTL = time.Minute

var cache = ttlcache.New(cacheTTL)

// email returns the user email from a Vanadium blessing that was produced via
// a BlessGoogle call.
var (
	groupRE *regexp.Regexp
	userRE  *regexp.Regexp
)

func googleGroupsInit(googleUserSufix string, googleGroupSufix string) {
	groupRE = regexp.MustCompile(strings.Join([]string{"^" + "googlegroups", fmt.Sprintf("([a-z0-9-_+.]+@%s)$", googleGroupSufix)}, security.ChainSeparator))
	userRE = regexp.MustCompile(strings.Join([]string{"^" + extensionPrefix, fmt.Sprintf("([a-z0-9-_+.]+@%s)", googleUserSufix)}, security.ChainSeparator))
}

//
// For example, for 'v23.grail.com:google:razvanm@grailbio.com' the return
// string should be 'razvanm@grailbio.com'.
func email(blessing string, prefix string) string {
	if strings.HasPrefix(blessing, prefix) && blessing != prefix {
		m := userRE.FindStringSubmatch(blessing[len(prefix)+1:])
		if m != nil {
			return m[1]
		}
	}
	return ""
}

// group returns the Google Groups name from a Vanadium blessing.
//
// For example, for 'v23.grail.com:googlegroups:eng@grailbio.com' the return
// string should be 'eng@grailbio.com'.
func group(blessing string, prefix string) string {
	vlog.Infof("blessing %q %q", blessing, prefix)
	if strings.HasPrefix(blessing, prefix) {
		m := groupRE.FindStringSubmatch(blessing[len(prefix)+1:])
		if m != nil {
			return m[1]
		}
	}
	return ""
}

type authorizer struct {
	perms   access.Permissions
	tagType *vdl.Type
	// isMember checks if a user is member of a particular Google Group.
	isMember func(user, group string) bool
}

func googleGroupsAuthorizer(perms access.Permissions, jwtConfig *jwt.Config, groupLookupName string, googleUserSufix string, googleGroupSufix string) security.Authorizer {
	googleGroupsInit(googleUserSufix, googleGroupSufix)
	return &authorizer{
		perms:   perms,
		tagType: access.TypicalTagType(),
		isMember: func(user, group string) bool {
			key := cacheKey{user, group}
			if v, ok := cache.Get(key); ok {
				vlog.VI(1).Infof("cache hit for %+v", key)
				return v.(bool)
			}
			vlog.VI(1).Infof("cache miss for %+v", key)

			config := *jwtConfig
			// This needs to be a Super Admin of the domain.
			config.Subject = groupLookupName
			service, err := admin.New(config.Client(context.Background()))
			if err != nil {
				vlog.Info(err)
				return false
			}

			// If the group is in a different domain, perform a user based group membership check
			// This loses the ability to check for nested groups - see https://phabricator.grailbio.com/D13275
			// and https://github.com/googleapis/google-api-java-client/issues/1082
			if googleGroupSufix != googleUserSufix {
				member, member_err := admin.NewMembersService(service).Get(group, user).Do()
				if member_err != nil {
					vlog.Info(member_err)
					return false
				}
				vlog.Infof("member: %+v", member)
				isMember := member.Status == "ACTIVE"
				vlog.VI(1).Infof("add to cache %+v", key)
				cache.Set(key, isMember)
				return isMember
			}

			result, err := admin.NewMembersService(service).HasMember(group, user).Do()
			if err != nil {
				vlog.Info(err)
				return false
			}
			vlog.Infof("hasMember: %+v", result)

			vlog.VI(1).Infof("add to cache %+v", key)
			cache.Set(key, result.IsMember)

			return result.IsMember
		},
	}
}

func (a *authorizer) pruneBlessingslist(acl access.AccessList, blessings []string, localBlessings string) []string {
	if len(acl.NotIn) == 0 {
		return blessings
	}
	var filtered []string
	for _, b := range blessings {
		inDenyList := false
		for _, bp := range acl.NotIn {
			if security.BlessingPattern(bp).MatchedBy(b) {
				inDenyList = true
				break
			}
			userEmail := email(b, localBlessings)
			groupEmail := group(bp, localBlessings)
			vlog.Infof("%q %q", userEmail, groupEmail)
			if userEmail != "" && groupEmail != "" {
				if a.isMember(userEmail, groupEmail) {
					vlog.Infof("%q is a member of %q (NotIn blessing pattern %q)", userEmail, groupEmail, bp)
					inDenyList = true
					break
				}
			}
		}
		if !inDenyList {
			filtered = append(filtered, b)
		}
	}
	return filtered
}

func (a *authorizer) aclIncludes(acl access.AccessList, blessings []string, localBlessings string) bool {
	blessings = a.pruneBlessingslist(acl, blessings, localBlessings)
	for _, pattern := range acl.In {
		if pattern.MatchedBy(blessings...) {
			return true
		}
		for _, b := range blessings {
			userEmail := email(b, localBlessings)
			groupEmail := group(string(pattern), localBlessings)
			vlog.Infof("%q %q", userEmail, groupEmail)
			if userEmail != "" && groupEmail != "" {
				if a.isMember(userEmail, groupEmail) {
					vlog.Infof("%q is a member of %q (In blessing pattern %q)", userEmail, groupEmail, pattern)
					return true
				}
			}
		}
	}
	return false
}

func (a *authorizer) Authorize(ctx *v23context.T, call security.Call) error {
	blessings, invalid := security.RemoteBlessingNames(ctx, call)
	vlog.Infof("RemoteBlessingNames: %q Tags: %q", blessings, call.MethodTags())

	for _, tag := range call.MethodTags() {
		if tag.Type() == a.tagType {
			if acl, exists := a.perms[tag.RawString()]; !exists || !a.aclIncludes(acl, blessings, call.LocalBlessings().String()) {
				return access.NewErrNoPermissions(ctx, blessings, invalid, tag.RawString())
			}
		}
	}
	return nil
}
