package main

import (
	"fmt"
	"strings"

	"github.com/go-test/deep"
	groupssettings "google.golang.org/api/groupssettings/v1"
	"v.io/x/lib/cmdline"
	"v.io/x/lib/vlog"
)

func newGroup(g groupssettings.Groups) *groupssettings.Groups {
	return &groupssettings.Groups{
		Description: description(g.Email),

		AllowExternalMembers:        "false",
		AllowGoogleCommunication:    "true",
		AllowWebPosting:             "false",
		ArchiveOnly:                 "true",
		IncludeCustomFooter:         "false",
		IncludeInGlobalAddressList:  "true",
		IsArchived:                  "true",
		MembersCanPostAsTheGroup:    "false",
		MessageModerationLevel:      "MODERATE_NONE",
		ReplyTo:                     "REPLY_TO_IGNORE",
		SendMessageDenyNotification: "false",
		ShowInGroupDirectory:        "true",
		SpamModerationLevel:         "MODERATE",
		WhoCanAdd:                   "ALL_MANAGERS_CAN_ADD",
		WhoCanContactOwner:          "ALL_IN_DOMAIN_CAN_CONTACT",
		WhoCanInvite:                "ALL_MANAGERS_CAN_INVITE",
		WhoCanJoin:                  "CAN_REQUEST_TO_JOIN",
		WhoCanLeaveGroup:            "ALL_MEMBERS_CAN_LEAVE",
		WhoCanPostMessage:           "NONE_CAN_POST",
		WhoCanViewGroup:             "ALL_IN_DOMAIN_CAN_VIEW",
		WhoCanViewMembership:        "ALL_IN_DOMAIN_CAN_VIEW",

		Email:              g.Email,
		Kind:               g.Kind,
		MaxMessageBytes:    g.MaxMessageBytes,
		MessageDisplayFont: g.MessageDisplayFont,
		Name:               g.Name,
	}
}

func runUpdate(_ *cmdline.Env, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("bad number of arguments, expected 1, got %q", args)
	}
	groupName := args[0]
	if !Any(groupSuffix, func(v string) bool {
		return strings.HasSuffix(groupName, v)
	}) {
		return fmt.Errorf("bad suffix: the group name %q doesn't end in %q", groupName, groupSuffix)
	}

	service, err := newGroupsSettingsService()
	if err != nil {
		return err
	}

	origGroup, err := service.Groups.Get(groupName).Do()
	if err != nil {
		return fmt.Errorf("groupssettings.Group.Get(%q): %v", groupName, err)
	}
	// We need to zero some fields to make the diff nicer.
	origGroup.ServerResponse.HTTPStatusCode = 0
	origGroup.ServerResponse.Header = nil

	group := newGroup(*origGroup)
	if !descriptionFlag {
		group.Description = origGroup.Description
	}
	diff := deep.Equal(origGroup, group)
	if len(diff) == 0 {
		fmt.Println("No diffs")
		return nil
	}

	fmt.Printf("Found %d diffs:\n\n", len(diff))
	for _, l := range diff {
		fmt.Printf("\t%s\n", l)
	}
	fmt.Println()

	if dryRunFlag {
		return nil
	}
	updatedGroup, err := service.Groups.Update(groupName, group).Do()
	vlog.Infof("%#v", updatedGroup)
	if err != nil {
		return fmt.Errorf("groupssettings.Group.Update(%q): %v", groupName, err)
	}
	return nil
}
