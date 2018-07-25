package main

import (
	"fmt"
	"strings"

	"v.io/x/lib/vlog"

	"google.golang.org/api/admin/directory/v1"

	"v.io/x/lib/cmdline"
)

func runCreate(_ *cmdline.Env, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("bad number of arguments, expected 1, got %q", args)
	}
	groupName := args[0]
	if !strings.HasSuffix(groupName, groupSuffix) {
		return fmt.Errorf("bad suffix: the group name %q doesn't end in %q", groupName, groupSuffix)
	}

	adminService, err := newAdminService()
	if err != nil {
		return err
	}

	group, err := adminService.Groups.Insert(&admin.Group{
		Email:       groupName,
		Description: description(groupName),
	}).Do()
	if err != nil {
		return err
	}
	vlog.Infof("%+v\n", group)

	groupssettingService, err := newGroupsSettingsService()
	if err != nil {
		return err
	}

	settingsGroup, err := groupssettingService.Groups.Get(groupName).Do()
	if err != nil {
		return fmt.Errorf("groupssettings.Group.Get(%q): %v", groupName, err)
	}
	settingsGroup = newGroup(*settingsGroup)
	updatedGroup, err := groupssettingService.Groups.Update(groupName, settingsGroup).Do()
	vlog.Infof("%#v", updatedGroup)
	if err != nil {
		return fmt.Errorf("groupssettings.Group.Update(%q): %v", groupName, err)
	}
	return nil
}
