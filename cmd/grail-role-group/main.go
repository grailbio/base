// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// The following enables go generate to generate the doc.go file.
//go:generate go run v.io/x/lib/cmdline/gendoc "--build-cmd=go install" --copyright-notice= . -help
package main

import (
	"net/http"
	"os"

	"github.com/grailbio/base/cmd/grail-role-group/googleclient"

	"github.com/grailbio/base/cmdutil"
	_ "github.com/grailbio/base/cmdutil/interactive"
	"golang.org/x/oauth2"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/groupssettings/v1"
	"v.io/x/lib/cmdline"
)

const (
	// DATA(sensitive): These are the OAuth2 client ID and secret. They were
	// generated in the grail-role-group Google Cloud Project. The client secret
	// is not secret in this case because it is part of client tool. It does act
	// as an identifier that allows restriction based on quota on the Google
	// side.
	clientID     = "961318960823-f1h3iobupln4959to1ja13895htiiah5.apps.googleusercontent.com"
	clientSecret = "i7ANm8RJy-7Y0oOP1uV-yKPU"
)

const domain = "grailbio.com"

var groupSuffix = []string{"-aws-role@grailbio.com", "-ticket@grailbio.com"}

var (
	browserFlag     bool
	dryRunFlag      bool
	descriptionFlag bool
)

func newClient() (*http.Client, error) {
	return googleclient.New(googleclient.Options{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Scopes:       scopes,
		// We request online only to avoid caching elevated refresh tokens for too
		// long.
		AccessType:  oauth2.AccessTypeOnline,
		ConfigFile:  os.ExpandEnv("${HOME}/.config/grail-role-group/credentials.json"),
		OpenBrowser: true,
	})
}

func newAdminService() (*admin.Service, error) {
	client, err := newClient()
	if err != nil {
		return nil, err
	}
	return admin.New(client)
}

func newGroupsSettingsService() (*groupssettings.Service, error) {
	client, err := newClient()
	if err != nil {
		return nil, err
	}
	return groupssettings.New(client)
}

func newCmdRoot() *cmdline.Command {
	cmd := &cmdline.Command{
		Name:     "role-group",
		Short:    "Manage Google Groups used in ticket-server config files",
		LookPath: false,
		Children: []*cmdline.Command{
			newCmdList(),
			newCmdCreate(),
			newCmdUpdate(),
		},
	}
	return cmd
}

func newCmdList() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner: cmdutil.RunnerFunc(runList),
		Name:   "list",
		Short:  "List all the role groups",
	}
	cmd.Flags.BoolVar(&browserFlag, "browser", true, "Attempt to open a browser.")
	return cmd
}

func newCmdCreate() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner:   cmdutil.RunnerFunc(runCreate),
		Name:     "create",
		Short:    "Create a new role group",
		ArgsName: "<role name>",
	}
	cmd.Flags.BoolVar(&browserFlag, "browser", true, "Attempt to open a browser.")
	cmd.Flags.BoolVar(&descriptionFlag, "description", true, "Compose a standard description.")
	return cmd
}

func newCmdUpdate() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner:   cmdutil.RunnerFunc(runUpdate),
		Name:     "update",
		Short:    "Update an existing role group",
		ArgsName: "<role name>",
	}
	cmd.Flags.BoolVar(&browserFlag, "browser", true, "Attempt to open a browser.")
	cmd.Flags.BoolVar(&descriptionFlag, "description", true, "Compose a standard description.")
	cmd.Flags.BoolVar(&dryRunFlag, "dry-run", true, "Safeguard to avoid accidental updates.")
	return cmd
}

// Any return true if any string in list returns true based on the passed comparison method
func Any(vs []string, f func(string) bool) bool {
	for _, v := range vs {
		if f(v) {
			return true
		}
	}
	return false
}

func main() {
	cmdline.HideGlobalFlagsExcept()
	cmdline.Main(newCmdRoot())
}
