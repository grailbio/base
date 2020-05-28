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
	"strings"
	"sync"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/security/identity"
	"github.com/grailbio/base/web/webutil"
	"golang.org/x/oauth2"
	goauth2 "google.golang.org/api/oauth2/v1"
	vcontext "v.io/v23/context"
	"v.io/v23/security"
	"v.io/x/lib/vlog"
)

func fetchGoogleBlessings(ctx *vcontext.T) (security.Blessings, error) {
	idToken, err := fetchIDToken(ctx)
	if err != nil {
		return security.Blessings{}, err
	}
	stub := identity.GoogleBlesserClient(blesserGoogleFlag)
	return stub.BlessGoogle(ctx, idToken)
}

// fetchIDToken obtains a Google ID Token using an OAuth2 flow with Google. The
// user will be instructed to use and URL or a browser will automatically open.
func fetchIDToken(ctx context.Context) (string, error) {
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
			log.Fatalf("Bad state: got %q, want %q", got, want)
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
			AuthURL:  googleOauth2Flag + "/v2/auth",
			TokenURL: googleOauth2Flag + "/token",
		},
	}

	url := config.AuthCodeURL(state, oauth2.AccessTypeOnline)

	if browserFlag {
		fmt.Printf("Opening %q...\n", url)
		if webutil.StartBrowser(url) {
			wg.Wait()
			if err = server.Shutdown(ctx); err != nil {
				vlog.Errorf("shutting down: %v", err)
			}
		} else {
			browserFlag = false
		}
	}

	if !browserFlag {
		config.RedirectURL = "urn:ietf:wg:oauth:2.0:oob"
		url := config.AuthCodeURL(state, oauth2.AccessTypeOnline)
		fmt.Printf("The attempt to automatically open a browser failed. Please open the following link:\n\n\t%s\n\n", url)
		fmt.Printf("Paste the received code and then press enter: ")
		if _, err := fmt.Scanf("%s", &code); err != nil {
			return "", err
		}
		fmt.Println("")
	}

	vlog.VI(1).Infof("code: %+v", code)
	token, err := config.Exchange(ctx, code)
	if err != nil {
		return "", err
	}
	vlog.VI(1).Infof("ID token: +%v", token.Extra("id_token").(string))
	return token.Extra("id_token").(string), nil
}
