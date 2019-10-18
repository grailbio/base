// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package dump

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"sort"
	"sync"
	"testing"
)

func makeDumpConst(errC chan<- error, s string) Func {
	return func(ctx context.Context, w io.Writer) error {
		if _, err := w.Write([]byte(s)); err != nil {
			// This should not happen, so we let the main test goroutine know.
			errC <- err
		}
		return nil
	}
}

func makeDumpError(errC chan<- error, s string) Func {
	return func(ctx context.Context, w io.Writer) error {
		// Fake a partial failed write.
		s := s[:len(s)/2]
		if _, err := w.Write([]byte(s)); err != nil {
			// This should not happen, so we let the main test goroutine know.
			errC <- err
		}
		return errors.New("dump func error")
	}
}

func TestShellQuote(t *testing.T) {
	for _, c := range []struct {
		s    string
		want string
	}{
		{``, `''`},
		{`'`, `''\'''`},
		{`hello`, `'hello'`},
		{`hello world`, `'hello world'`},
		{`hello'world`, `'hello'\''world'`},
	} {
		if got, want := shellQuote(c.s), c.want; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func verifyDump(t *testing.T, server *httptest.Server, dumpFuncErrC chan error, wantNames []string) {
	var dumpFuncErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dumpFuncErr = <-dumpFuncErrC
	}()

	resp, err := http.Get(server.URL + "/dump")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got, want := resp.StatusCode, http.StatusOK; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// Read the whole body, so we can immediately make sure that our dump
	// funcs worked.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("could not read dump body: %v", err)
	}
	close(dumpFuncErrC)
	wg.Wait()
	if dumpFuncErr != nil {
		t.Fatalf("unexpected error writing dump part: %v", dumpFuncErr)
	}
	gzr, err := gzip.NewReader(bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("gzip.NewReader failed: %v", err)
	}
	tr := tar.NewReader(gzr)
	re := regexp.MustCompile(`.*/`)
	var names []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("error from (*TarReader).Next(): %v", err)
		}
		// Strip the prefix to recover the original name.
		name := re.ReplaceAllString(hdr.Name, "")
		names = append(names, name)
		var contents bytes.Buffer
		if _, err := io.Copy(&contents, tr); err != nil {
			t.Fatal(err)
		}
		// Assume contents are "<name>-contents", matching our known
		// construction of the dump contents.
		if got, want := contents.String(), name+"-contents"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	sort.Strings(names)
	sort.Strings(wantNames)
	if got, want := names, wantNames; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestServeHTTP(t *testing.T) {
	reg := NewRegistry("abc")
	dumpFuncErrC := make(chan error)
	reg.Register("foo", makeDumpConst(dumpFuncErrC, "foo-contents"))
	reg.Register("bar", makeDumpConst(dumpFuncErrC, "bar-contents"))
	reg.Register("baz", makeDumpConst(dumpFuncErrC, "baz-contents"))

	mux := http.NewServeMux()
	mux.Handle("/dump", reg)
	server := httptest.NewServer(mux)

	verifyDump(t, server, dumpFuncErrC, []string{"foo", "bar", "baz"})
}

func TestServeHTTPFailedParts(t *testing.T) {
	reg := NewRegistry("abc")
	dumpFuncErrC := make(chan error)
	reg.Register("foo", makeDumpConst(dumpFuncErrC, "foo-contents"))
	// Note that the following dump part funcs will return an error.
	reg.Register("bar", makeDumpError(dumpFuncErrC, "bar-contents"))
	reg.Register("baz", makeDumpError(dumpFuncErrC, "baz-contents"))

	mux := http.NewServeMux()
	mux.Handle("/dump", reg)
	server := httptest.NewServer(mux)

	// Verify that only the successful dump part func is in the dump.
	verifyDump(t, server, dumpFuncErrC, []string{"foo"})
}
