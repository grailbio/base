// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build linux darwin

package webutil

import (
	"os"
	"os/exec"
	"runtime"
)

// StartBrowser tries to open the URL in a browser and reports whether it
// succeeds.
func StartBrowser(url string) bool {
	// try to start the browser
	var args []string
	aws_env := os.Getenv("AWS_ENV")
	switch runtime.GOOS {
	case "darwin":
		if aws_env != "" {
			args = []string{"open", "-na", "Google Chrome", "--args", "--profile-directory=" + aws_env, "--new-window"}
		} else {
			args = []string{"open"}
		}
	case "windows":
		args = []string{"cmd", "/c", "start"}
	default:
		args = []string{"xdg-open"}
	}
	cmd := exec.Command(args[0], append(args[1:], url)...)
	return cmd.Start() == nil
}
