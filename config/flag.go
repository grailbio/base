// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/grailbio/base/backgroundcontext"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
)

type (
	// flags is an ordered representation of profile flags. Each entry (implementing flagEntry)
	// is a type of flag, and entry types may be interleaved. They're handled in the order
	// the user passed them.
	//
	// The flags object is wrapped for each entry type, and each wrapper's flag.Value implementation
	// appends the appropriate entry.
	flags struct {
		defaultProfilePath string
		entries            []flagEntry
	}
	flagsProfilePaths   flags
	flagsProfileInlines flags
	flagsSets           flags

	flagEntry interface {
		process(context.Context, *Profile) error
	}
	flagEntryProfilePath   struct{ string }
	flagEntryProfileInline struct{ string }
	flagEntrySet           struct{ key, value string }
)

var (
	_ flagEntry = flagEntryProfilePath{}
	_ flagEntry = flagEntryProfileInline{}
	_ flagEntry = flagEntrySet{}

	_ flag.Value = (*flagsProfilePaths)(nil)
	_ flag.Value = (*flagsProfileInlines)(nil)
	_ flag.Value = (*flagsSets)(nil)
)

func (e flagEntryProfilePath) process(ctx context.Context, p *Profile) error {
	return p.loadFile(ctx, e.string)
}
func (e flagEntryProfileInline) process(_ context.Context, p *Profile) error {
	return p.Parse(strings.NewReader(e.string))
}
func (e flagEntrySet) process(_ context.Context, p *Profile) error {
	return p.Set(e.key, e.value)
}

func (f *flagsProfilePaths) String() string { return f.defaultProfilePath }
func (*flagsProfileInlines) String() string { return "" }
func (*flagsSets) String() string           { return "" }

func (f *flagsProfilePaths) Set(s string) error {
	if s == "" {
		return errors.New("empty path to profile")
	}
	f.entries = append(f.entries, flagEntryProfilePath{s})
	return nil
}
func (f *flagsProfileInlines) Set(s string) error {
	if s != "" {
		f.entries = append(f.entries, flagEntryProfileInline{s})
	}
	return nil
}
func (f *flagsSets) Set(s string) error {
	elems := strings.SplitN(s, "=", 2+1) // Split an additional part to detect errors.
	if len(elems) != 2 || elems[0] == "" {
		return fmt.Errorf("wrong argument format, expected key=value, got %q", s)
	}
	f.entries = append(f.entries, flagEntrySet{elems[0], elems[1]})
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
//	-profileinline text
//		Parses the argument. This is equivalent to writing the text to a file
//		and using -profile.
//
//	-profiledump
//		Writes the profile (after processing the above flags) to standard
//		error and exits.
//
// The flag names are prefixed with the provided prefix.
func (p *Profile) RegisterFlags(fs *flag.FlagSet, prefix string, defaultProfilePath string) {
	p.flags.defaultProfilePath = defaultProfilePath
	fs.Var((*flagsProfilePaths)(&p.flags), prefix+"profile", "load the profile at the provided path; may be repeated")
	fs.Var((*flagsSets)(&p.flags), prefix+"set", "set a profile parameter; may be repeated")
	fs.Var((*flagsProfileInlines)(&p.flags), prefix+"profileinline", "parse the profile passed as an argument; may be repeated")
	fs.BoolVar(&p.flagDump, "profiledump", false, "dump the profile to stderr and exit")
}

// NeedProcessFlags returns true when a call to p.ProcessFlags should
// not be delayed -- i.e., the flag values have user-visible side effects.
func (p *Profile) NeedProcessFlags() bool {
	return p.flagDump
}

func (f *flags) hasProfilePathEntry() bool {
	for _, entry := range f.entries {
		if _, ok := entry.(flagEntryProfilePath); ok {
			return true
		}
	}
	return false
}

func (p *Profile) loadFile(ctx context.Context, path string) (err error) {
	f, err := file.Open(ctx, path)
	if err != nil {
		return err
	}
	defer errors.CleanUpCtx(ctx, f.Close, &err)
	return p.Parse(f.Reader(ctx))
}

// ProcessFlags processes the flags as registered by RegisterFlags,
// and is documented by that method.
func (p *Profile) ProcessFlags() error {
	ctx := backgroundcontext.Get()
	if p.flags.defaultProfilePath != "" && !p.flags.hasProfilePathEntry() {
		if err := p.loadFile(ctx, p.flags.defaultProfilePath); err != nil {
			if !errors.Is(errors.NotExist, err) {
				return err
			}
		}
	}
	for _, entry := range p.flags.entries {
		if err := entry.process(ctx, p); err != nil {
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
