// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/grailbio/base/security/ticket"
	"v.io/v23/naming"
	"v.io/v23/security/access"
	"v.io/x/lib/vlog"
	"v.io/x/ref/lib/vdl/build"
	"v.io/x/ref/lib/vdl/compile"
	"v.io/x/ref/lib/vdl/vdlutil"
)

const configSuffix = ".vdlconfig"

type ticketConfig struct {
	Kind   string
	Ticket ticket.Ticket
	Perms  access.Permissions
}

type Config map[string]ticketConfig

// Load returns a map with the tickets from a directory where the key is the
// path of the ticket.
func Load(dir string) (map[string]ticketConfig, error) {
	all := Config{}

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		// The prefix we want for a path like 'config/docker/rwc.vdlconfig' is
		// 'docker/rwc' where 'config' is the dir variable. For this example dirPart
		// will be 'docker' and filePart will be 'rwc'.
		dirPart := strings.TrimLeft(strings.TrimPrefix(filepath.Dir(path), dir), "/")
		filePart := strings.TrimSuffix(filepath.Base(path), configSuffix)
		prefix := naming.Join(dirPart, filePart)
		vlog.VI(1).Infof("walk: %q prefix %q", path, prefix)

		if !strings.HasSuffix(path, configSuffix) {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("bad path %q: %+v", path, err)
		}

		errors := vdlutil.Errors{}
		packages := build.TransitivePackagesForConfig(path, f, build.Opts{}, &errors)
		env := compile.NewEnv(100)
		for _, p := range packages {
			vlog.VI(1).Infof("building package: %+v", p.Path)
			build.BuildPackage(p, env)
			if env.Errors.NumErrors() != 0 {
				return fmt.Errorf("errors building %s:\n%+v", p.Path, env.Errors)
			}
		}

		f.Seek(0, 0)
		config := ticket.Config{}
		build.BuildConfigValue(path, f, nil, env, &config)

		if env.Errors != nil && env.Errors.NumErrors() > 0 {
			return env.Errors.ToError()
		}

		for name, t := range config.Tickets {
			perms := config.Permissions
			if t.Permissions != nil {
				perms = t.Permissions
			}
			all[naming.Join(prefix, name)] = ticketConfig{
				Ticket: t.Ticket,
				Perms:  perms,
			}
		}

		return nil
	})

	return all, err
}
