package main

import (
	"context"
	"fmt"
	"strings"

	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/groupssettings/v1"
	goauth2 "google.golang.org/api/oauth2/v1"
	"v.io/x/lib/cmdline"
)

var scopes = []string{
	goauth2.UserinfoEmailScope,
	admin.AdminDirectoryGroupScope,
	admin.AdminDirectoryGroupMemberScope,
	admin.AdminDirectoryGroupReadonlyScope,
	groupssettings.AppsGroupsSettingsScope,
}

func runList(_ *cmdline.Env, args []string) error {
	service, err := newAdminService()
	if err != nil {
		return err
	}
	ctx := context.Background()
	return service.Groups.List().Domain(domain).Pages(ctx, func(groups *admin.Groups) error {
		for _, g := range groups.Groups {
			if Any(groupSuffix, func(v string) bool {
				return strings.HasSuffix(g.Email, v)
			}) {
				fmt.Printf("%v\n", g.Email)
			}
		}
		return nil
	})
}
