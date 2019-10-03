// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

type listFlag struct {
	defaultValue string
	values       *[]string
	needEqual    bool
}

func (l *listFlag) String() string { return l.defaultValue }

func (l *listFlag) Set(value string) error {
	if l.needEqual && !strings.Contains(value, "=") {
		return fmt.Errorf("invalid flag value %s: missing '='", value)
	}
	*l.values = append(*l.values, value)
	return nil
}

// RegisterFlags registers a set of flags on the provided FlagSet.
// These flags configure the profile when ProcessFlags is called
// (after flag parsing). The flags are:
//
// 	-profile path
//		Parses and loads the profile at the given path. This flag may be
//		repeated, loading each profile in turn. If no -profile flags are
//		specified, then the provided default path is loaded instead. If
//		the default path does not exist, it is skipped; other profile loading
//		errors cause ProcessFlags to return an error.
//
//	-set key=value
//		Sets the value of the named parameter. See Profile.Set for
//		details. This flag may be repeated.
//
//	-profiledump
//		Writes the profile (after processing the above flags) to standard
//		error and exits.
//
// The flag names are prefixed with the provided prefix.
func (p *Profile) RegisterFlags(fs *flag.FlagSet, prefix string, defaultProfilePath string) {
	p.flagDefaultPath = defaultProfilePath
	fs.Var(&listFlag{p.flagDefaultPath, &p.flagPaths, false}, prefix+"profile", "load the profile at the provided path; may be repeated")
	fs.Var(&listFlag{"", &p.flagParams, true}, prefix+"set", "set a profile parameter; may be repeated")
	fs.BoolVar(&p.flagDump, "profiledump", false, "dump the profile to stderr and exit")
}

// NeedProcessFlags returns true when a call to p.ProcessFlags should
// not be delayed -- i.e., the flag values have user-visible side effects.
func (p *Profile) NeedProcessFlags() bool {
	return p.flagDump
}

// ProcessFlags processes the flags as registered by RegisterFlags,
// and is documented by that method.
func (p *Profile) ProcessFlags() error {
	if len(p.flagPaths) == 0 && p.flagDefaultPath != "" {
		if f, err := os.Open(p.flagDefaultPath); err == nil {
			defer f.Close()
			if err := p.Parse(f); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	for _, path := range p.flagPaths {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := p.Parse(f); err != nil {
			return err
		}
	}
	for _, param := range p.flagParams {
		elems := strings.SplitN(param, "=", 2)
		if len(elems) != 2 {
			panic(param)
		}
		if err := p.Set(elems[0], elems[1]); err != nil {
			return err
		}
	}
	if p.flagDump {
		// TODO(marius): also prune uninstantiable instances?
		for _, inst := range p.sorted() {
			if len(inst.params) == 0 && inst.parent == "" {
				continue
			}
			// Resolve each known key.
			var docs map[string]string
			if global := p.globals[inst.name]; global != nil {
				// Populate the parameter docs.
				docs = make(map[string]string)
				for name, param := range global.params {
					docs[name] = param.help
				}
			}
			fmt.Fprintln(os.Stderr, inst.SyntaxString(docs))
		}
		os.Exit(1)
	}
	return nil
}

// RegisterFlags registers the default profile on flag.CommandLine
// with the provided prefix. See Profile.RegisterFlags for details.
func RegisterFlags(prefix string, defaultProfilePath string) {
	Application().RegisterFlags(flag.CommandLine, prefix, defaultProfilePath)
}

// ProcessFlags processes the flags as registered by RegisterFlags.
func ProcessFlags() error {
	return Application().ProcessFlags()
}
