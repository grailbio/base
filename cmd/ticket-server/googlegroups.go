// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/grailbio/base/common/log"
	"github.com/grailbio/base/ttlcache"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/jwt"
	admin "google.golang.org/api/admin/directory/v1"
	v23context "v.io/v23/context"
	"v.io/v23/security"
	"v.io/v23/security/access"
	"v.io/v23/vdl"
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
	groupRE           *regexp.Regexp
	userRE            *regexp.Regexp
	adminLookupDomain string
)

func googleGroupsInit(ctx *v23context.T, groupLookupName string) {
	if hostedDomains == nil || len(hostedDomains) == 0 {
		log.Error(ctx, "hostedDomains not initialized.")
		panic("hostedDomains not initialized")
	}

	// Extract the domain of the admin account to filter users in the same Google Domain
	adminLookupDomain = emailDomain(groupLookupName)
	groupRE = regexp.MustCompile(strings.Join([]string{"^" + "googlegroups", fmt.Sprintf("([a-z0-9-_+.]+@[a-z0-9-_+.]+)$")}, security.ChainSeparator))
	// NOTE This is a non terminating string, because the user validation can be terminated by the ChainSeparator (`:`)
	userRE = regexp.MustCompile(strings.Join([]string{"^" + extensionPrefix, fmt.Sprintf("([a-z0-9-_+.]+@[a-z0-9-_+.]+)")}, security.ChainSeparator))
}

//verifyAndExtractEmailFromBlessing returns the email address defined in a v23 principal/blessing
//
// For example, for 'v23.grail.com:google:razvanm@grailbio.com' the return
// string should be 'razvanm@grailbio.com'.
func verifyAndExtractEmailFromBlessing(blessing string, prefix string) string {
	if strings.HasPrefix(blessing, prefix) && blessing != prefix {
		m := userRE.FindStringSubmatch(blessing[len(prefix)+1:])
		if m != nil && stringInSlice(hostedDomains, emailDomain(m[1])) {
			return m[1]
		}
	}
	return ""
}

// extractGroupEmailFromBlessing returns the Google Groups name from a Vanadium blessing.
//
// For example, for 'v23.grail.com:googlegroups:eng@grailbio.com' the return
// string should be 'eng@grailbio.com'.
func extractGroupEmailFromBlessing(ctx *v23context.T, blessing string, prefix string) string {
	log.Debug(ctx, "extracting group email from blessing", "blessing", blessing, "prefix", prefix)
	if strings.HasPrefix(blessing, prefix) {
		m := groupRE.FindStringSubmatch(blessing[len(prefix)+1:])

		if m != nil && stringInSlice(hostedDomains, emailDomain(m[1])) {
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

func (a *authorizer) String() string {
	return fmt.Sprintf("%+v", a.perms)
}

func googleGroupsAuthorizer(ctx *v23context.T, perms access.Permissions, jwtConfig *jwt.Config,
	groupLookupName string) security.Authorizer {
	googleGroupsInit(ctx, groupLookupName)
	return &authorizer{
		perms:   perms,
		tagType: access.TypicalTagType(),
		isMember: func(user, group string) bool {
			key := cacheKey{user, group}
			if v, ok := cache.Get(key); ok {
				log.Debug(ctx, "Google groups lookup cache hit", "key", key)
				return v.(bool)
			}
			log.Debug(ctx, "Google groups lookup cache miss", "key", key)

			config := *jwtConfig
			// This needs to be a Super Admin of the domain.
			config.Subject = groupLookupName

			service, err := admin.New(config.Client(context.Background()))
			if err != nil {
				log.Error(ctx, err.Error())
				return false
			}

			// If the group is in a different domain, perform a user based group membership check
			// This loses the ability to check for nested groups - see https://phabricator.grailbio.com/D13275
			// and https://github.com/googleapis/google-api-java-client/issues/1082
			if adminLookupDomain != emailDomain(user) {
				member, member_err := admin.NewMembersService(service).Get(group, user).Do()
				if member_err != nil {
					log.Error(ctx, member_err.Error())
					return false
				}
				log.Debug(ctx, "adding member to cache", "member", member, "key", key)
				isMember := member.Status == "ACTIVE"
				cache.Set(key, isMember)
				return isMember
			}

			result, err := admin.NewMembersService(service).HasMember(group, user).Do()
			if err != nil {
				log.Error(ctx, err.Error())
				return false
			}
			log.Debug(ctx, "adding member to cache", "hasMember", result, "key", key)
			cache.Set(key, result.IsMember)

			return result.IsMember
		},
	}
}

func (a *authorizer) pruneBlessingslist(ctx *v23context.T, acl access.AccessList, blessings []string, localBlessings string) []string {
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
			userEmail := verifyAndExtractEmailFromBlessing(b, localBlessings)
			groupEmail := extractGroupEmailFromBlessing(ctx, bp, localBlessings)
			log.Debug(ctx, "pruning blessings list", "userEmail", userEmail, "groupEmail", groupEmail)
			if userEmail != "" && groupEmail != "" {
				if a.isMember(userEmail, groupEmail) {
					log.Debug(ctx, "user is a member of group", "userEmail", userEmail, "groupEmail", groupEmail,
						"blessingPattern", bp)
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

func (a *authorizer) aclIncludes(ctx *v23context.T, acl access.AccessList, blessings []string,
	localBlessings string) bool {
	blessings = a.pruneBlessingslist(ctx, acl, blessings, localBlessings)
	for _, bp := range acl.In {
		if bp.MatchedBy(blessings...) {
			return true
		}
		for _, b := range blessings {
			userEmail := verifyAndExtractEmailFromBlessing(b, localBlessings)
			groupEmail := extractGroupEmailFromBlessing(ctx, string(bp), localBlessings)
			log.Debug(ctx, "checking access list", "userEmail", userEmail, "groupEmail", groupEmail)
			if userEmail != "" && groupEmail != "" {
				if a.isMember(userEmail, groupEmail) {
					log.Debug(ctx, "user is a member of group", "userEmail", userEmail, "groupEmail", groupEmail,
						"blessingPattern", bp)
					return true
				}
			}
		}
	}
	return false
}

func (a *authorizer) Authorize(ctx *v23context.T, call security.Call) error {
	blessings, invalid := security.RemoteBlessingNames(ctx, call)
	log.Debug(ctx, "authorizing via Google flow", "blessings", blessings, "tags", call.MethodTags())

	for _, tag := range call.MethodTags() {
		if tag.Type() == a.tagType {
			if acl, exists := a.perms[tag.RawString()]; !exists || !a.aclIncludes(ctx, acl, blessings,
				call.LocalBlessings().String()) {
				return access.ErrorfNoPermissions(ctx, "%v %v %v", blessings, invalid, tag.RawString())
			}
		}
	}
	return nil
}
