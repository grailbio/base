// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/grailbio/base/must"
)

// This test uses a mock "app" to demonstrate various aspects of package config.

type credentials interface {
	Creds() string
}

type userCredentials string

func (u userCredentials) Creds() string { return string(u) }

type envCredentials struct{}

func (e envCredentials) Creds() string { return "environment" }

type database struct {
	table string
	creds credentials
}

type frontend struct {
	db    database
	creds credentials
	limit int
}

func init() {
	Register("app/auth/env", func(constr *Constructor) {
		constr.New = func() (interface{}, error) {
			return envCredentials{}, nil
		}
	})
	Register("app/auth/login", func(constr *Constructor) {
		var (
			username = constr.String("user", "test", "the username")
			password = constr.String("password", "secret", "the password")
		)
		constr.New = func() (interface{}, error) {
			return userCredentials(fmt.Sprintf("%s:%s", *username, *password)), nil
		}
	})

	Register("app/database", func(constr *Constructor) {
		var db database
		constr.StringVar(&db.table, "table", "defaulttable", "the database table")
		constr.InstanceVar(&db.creds, "credentials", "app/auth/env", "credentials used for database access")
		constr.New = func() (interface{}, error) {
			if db.creds == nil {
				return nil, errors.New("credentials not defined")
			}
			return db, nil
		}
	})

	Register("app/frontend", func(constr *Constructor) {
		var fe frontend
		constr.InstanceVar(&fe.db, "database", "app/database", "the database to be used")
		constr.InstanceVar(&fe.creds, "credentials", "app/auth/env", "credentials to use for authentication")
		constr.IntVar(&fe.limit, "limit", 128, "maximum number of concurrent requests to handle")
		constr.New = func() (interface{}, error) {
			if fe.db == (database{}) || fe.creds == nil {
				return nil, errors.New("missing configuration")
			}
			return fe, nil
		}
	})
}

func TestFlag(t *testing.T) {
	profile := func(args ...string) *Profile {
		t.Helper()
		p := New()
		f, err := os.Open("testdata/profile")
		must.Nil(err)
		defer f.Close()
		if err := p.Parse(f); err != nil {
			t.Fatal(err)
		}
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		p.RegisterFlags(fs, "", "testdata/profile")
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		if err := p.ProcessFlags(); err != nil {
			t.Fatal(err)
		}
		return p
	}

	expectCreds := func(fecreds, dbcreds string, args ...string) {
		t.Helper()
		p := profile(args...)
		var fe frontend
		if err := p.Instance("app/frontend", &fe); err != nil {
			t.Fatal(err)
		}
		if got, want := fe.creds.Creds(), fecreds; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := fe.db.creds.Creds(), dbcreds; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	expectCreds("marius:supersecret", "marius:supersecret")
	expectCreds("marius:public", "marius:public", "-set", "app/auth/login.password=public")
	expectCreds("environment", "marius:supersecret", "-set", "app/frontend.credentials=app/auth/env")
}
