// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build windows

package webutil

import (
	"fmt"
	"os/exec"
	"runtime"
	"syscall"
)

// StartBrowser tries to open the URL in a browser and reports whether it
// succeeds.
func StartBrowser(url string) bool {
	cmd := exec.Command("cmd")
	if runtime.GOOS == "windows" {
		cmd.SysProcAttr = &syscall.SysProcAttr{
			CmdLine: fmt.Sprintf(`cmd /c start "" "%s"`, url),
		}
	}
	return cmd.Start() == nil
}
