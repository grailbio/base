// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//+build !unit

package cmdutil_test

import (
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grailbio/testutil"
	"github.com/stretchr/testify/require"
	"v.io/x/lib/gosh"
)

func TestLogging(t *testing.T) {
	sh := gosh.NewShell(t)
	if testing.Verbose() {
		sh.PropagateChildOutput = true
	}
	tempdir := sh.MakeTempDir()
	defer testutil.NoCleanupOnError(t, sh.Cleanup, tempdir)
	logger := testutil.GoExecutable(t, "//go/src/github.com/grailbio/base/cmdutil/cmdline-test/cmdline-test")
	naked := testutil.GoExecutable(t, "//go/src/github.com/grailbio/base/cmdutil/naked-test/naked-test")
	testLogging(t, sh, tempdir, logger, naked)
	testHelp(t, sh, tempdir, logger, naked)
}

func testLogging(t *testing.T, sh *gosh.Shell, tempdir, logger, naked string) {
	for _, tc := range []struct {
		cmd    string
		args   []string
		prefix string
	}{
		{logger, []string{"logging"}, "T"},
		{logger, []string{"access"}, "T"},
		{naked, nil, "T"},
	} {
		args := append(tc.args, "--log_dir="+tempdir, "a", "b")
		cmd := sh.Cmd(tc.cmd, args...)
		out := cmd.CombinedOutput()
		if got, want := strings.TrimSpace(out), tempdir; got != want {
			t.Errorf("%v: got %v, want %v", tc.cmd, got, want)
		}
		data, err := ioutil.ReadFile(filepath.Join(tempdir, filepath.Base(tc.cmd)+".INFO"))
		require.NoError(t, err)
		if got, want := string(data), tc.prefix+": 0: a\n"; !strings.Contains(got, want) {
			t.Errorf("%v: got %v, does not contain %v", tc.cmd, got, want)
		}
		args = append(tc.args, "--log_dir="+tempdir, "--alsologtostderr", "a", "b")
		cmd = sh.Cmd(tc.cmd, args...)
		out = cmd.CombinedOutput()
		if got, want := out, tc.prefix+": 1: b\n"; !strings.Contains(got, want) {
			t.Errorf("%v: got %v, does not contain %v", tc.cmd, got, want)
		}
	}
}

func testHelp(t *testing.T, sh *gosh.Shell, tempdir, logger, naked string) {
	sh.ContinueOnError = true
	for _, test := range []struct {
		cmd      string
		contains []string
	}{
		{logger, []string{"cmdline-test [flags] <command>", "The global flags are:", "-alsologtostderr=false"}},
		{naked, []string{"Usage of ", "-alsologtostderr\n"}},
	} {
		cmd := sh.Cmd(test.cmd, "-help")
		got := cmd.CombinedOutput()
		for _, want := range test.contains {
			if !strings.Contains(got, want) {
				t.Errorf("%v: got %v, does not contain %v", test.cmd, got, want)
			}
		}
		sh.Err = nil
	}
	sh.ContinueOnError = false
}
