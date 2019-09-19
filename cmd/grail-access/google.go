// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/grailbio/base/cmdutil"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/security/identity"
	"github.com/grailbio/base/web/webutil"
	"golang.org/x/oauth2"
	goauth2 "google.golang.org/api/oauth2/v1"
	v23 "v.io/v23"
	v23context "v.io/v23/context"
	"v.io/v23/security"
	"v.io/x/lib/vlog"
	libsecurity "v.io/x/ref/lib/security"
)

func runGoogle(ctx *v23context.T) error {
	// TODO(razvanm): do we need to kill the v23agentd?

	// Best-effort cleanup.
	err := os.RemoveAll(credentialsDirFlag)
	if !os.IsNotExist(err) {
		vlog.Infof("remove %s: %v", credentialsDirFlag, err)
	}

	principal, err := libsecurity.CreatePersistentPrincipal(credentialsDirFlag, nil)
	if err != nil {
		return err
	}

	ctx, err = v23.WithPrincipal(ctx, principal)
	if err != nil {
		return err
	}

	idToken, err := fetchIDToken()
	if err != nil {
		return err
	}

	stub := identity.GoogleBlesserClient(blesserGoogleFlag)
	blessings, err := stub.BlessGoogle(ctx, idToken)
	if err != nil {
		return err
	}

	principal = v23.GetPrincipal(ctx)
	if err := principal.BlessingStore().SetDefault(blessings); err != nil {
		vlog.Error(err)
		return errors.E(err, "set default blessings")
	}
	_, err = principal.BlessingStore().Set(blessings, "...")
	if err != nil {
		vlog.Error(err)
		return errors.E(err, "set blessings")
	}
	if err := security.AddToRoots(principal, blessings); err != nil {
		return errors.E(err, "failed to add blessings to recognized roots: %v")
	}

	dump(ctx)

	return nil
}

// fetchIDToken obtains a Google ID Token using an OAuth2 flow with Google. The
// user will be instructed to use and URL or a browser will automatically open.
func fetchIDToken() (string, error) {
	stateBytes := make([]byte, 16)
	if _, err := rand.Read(stateBytes); err != nil {
		return "", err
	}
	state := hex.EncodeToString(stateBytes)

	code := ""
	wg := sync.WaitGroup{}
	wg.Add(1)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			return
		}
		if got, want := r.FormValue("state"), state; got != want {
			cmdutil.Fatalf("Bad state: got %q, want %q", got, want)
		}
		code = r.FormValue("code")
		w.Header().Set("Content-Type", "text/html")
		// JavaScript only allows closing windows/tab that were open via
		// JavaScript.
		_, _ = fmt.Fprintf(w, `<html><body>Code received. Please close this tab/window.</body></html>`)
		wg.Done()
	})

	ln, err := net.Listen("tcp", "localhost:")
	if err != nil {
		return "", err
	}
	vlog.Infof("listening: %v\n", ln.Addr().String())
	port := strings.Split(ln.Addr().String(), ":")[1]
	server := http.Server{Addr: "localhost:"}
	go server.Serve(ln.(*net.TCPListener)) // nolint: errcheck

	config := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Scopes:       []string{goauth2.UserinfoEmailScope},
		RedirectURL:  fmt.Sprintf("http://localhost:%s", port),
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://accounts.google.com/o/oauth2/v2/auth",
			TokenURL: "https://accounts.google.com/o/oauth2/token",
		},
	}

	url := config.AuthCodeURL(state, oauth2.AccessTypeOnline)

	if browserFlag {
		fmt.Printf("Opening %q...\n", url)
		if webutil.StartBrowser(url) {
			wg.Wait()
			if err = server.Shutdown(context.Background()); err != nil {
				vlog.Errorf("shutting down: %v", err)
			}
		} else {
			browserFlag = false
		}
	}

	if !browserFlag {
		config.RedirectURL = "urn:ietf:wg:oauth:2.0:oob"
		url := config.AuthCodeURL(state, oauth2.AccessTypeOnline)
		fmt.Printf("The attempt to automatically open a browser failed. Please open the following link in your browse:\n\n\t%s\n\n", url)
		fmt.Printf("Paste the received code and then press enter: ")
		if _, err := fmt.Scanf("%s", &code); err != nil {
			return "", err
		}
		fmt.Println("")
	}

	vlog.VI(1).Infof("code: %+v", code)
	token, err := config.Exchange(context.Background(), code)
	if err != nil {
		return "", err
	}
	vlog.VI(1).Infof("ID token: +%v", token.Extra("id_token").(string))
	return token.Extra("id_token").(string), nil
}
