// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main_test

import (
	"bufio"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/testutil"
	"github.com/stretchr/testify/require"
	"v.io/x/lib/gosh"
)

func retry(url string, duration time.Duration, iterations int) error {
	i := 0
	for {
		if iterations == i {
			return errors.New("max attempts exceeded for URL")
		}

		i++
		response, err := http.Get(url)
		if err == nil {
			if response.StatusCode == 200 {
				return nil
			}

			return errors.New("page not found")
		}

		time.Sleep(duration)
	}
}

func find(t *testing.T, dir string, files []string) []string {
	if len(files) == 0 {
		return nil
	}
	d, err := os.Open(dir)
	require.NoError(t, err)
	entries, err := d.Readdirnames(0)
	require.NoError(t, err)
	found := []string{}
	for _, want := range files {
		for _, f := range entries {
			if f == want {
				found = append(found, want)
				break
			}
		}
	}
	return found
}

func TestEverything(t *testing.T) {
	sh := gosh.NewShell(nil)
	bin := testutil.GoExecutable(t, "//go/src/github.com/grailbio/base/pprof/pprof-test/pprof-test")
	tempDir := sh.MakeTempDir()
	cpu := filepath.Join(tempDir, "c")
	heap := filepath.Join(tempDir, "h")

	for _, tst := range []struct {
		name    string
		args    []string
		files   []string
		success bool
	}{
		{"without", []string{}, nil, false},
		{"with", []string{"-pprof=127.0.0.1:0"}, nil, true},
		{"with-cl", []string{"-pprof=127.0.0.1:0", "-cpu-profile=" + cpu, "-heap-profile=" + heap}, []string{"c-00000.pprof", "h-00000.pprof"}, true},
	} {
		addrs := make(chan string)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			cmd := sh.Cmd(bin, tst.args...)
			rc := cmd.StdoutPipe()
			cmd.Start()
			require.NoError(t, cmd.Err)
			scanner := bufio.NewScanner(rc)
			for cmd.Err == nil && scanner.Scan() {
				a := scanner.Text()
				addrs <- a
			}
			rc.Close()
			cmd.Wait()
			close(addrs)
			wg.Done()
		}()

		mainAddr := <-addrs
		debugAddr := <-addrs

		// Make sure the main webserver is alive/test app is running.
		if err := retry("http://"+mainAddr+"/alive/", 100*time.Millisecond, 50); err != nil {
			t.Fatal(err)
		}

		// Make sure that /debug/ is not on the main webserver.
		if err := retry("http://"+mainAddr+"/debug/pprof/", 50*time.Millisecond, 1); err == nil {
			t.Fatal("found /debug/ on main server")
		}

		// Make sure that /debug/ is on the debug server, if configured.
		if err := retry("http://"+debugAddr+"/debug/pprof/", 100*time.Millisecond, 50); tst.success != (err == nil) {
			t.Fatalf("error detecting /debug/ %v %v %v %v %v", tst.success, err == nil, err, mainAddr, debugAddr)
		}

		// Quit the test app.
		http.Get("http://" + mainAddr + "/quitquitquit/")

		// Wait for the command to finish before testing for the files it
		// should have written.
		wg.Wait()

		if got, want := find(t, tempDir, tst.files), tst.files; !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v, want %v", tst.name, got, want)
		}
	}
}
