// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Command gofat is a simple utility to make fat binaries in the
// fatbin format (see github.com/grailbio/base/fatbin).
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"

	"github.com/grailbio/base/fatbin"
	"github.com/grailbio/base/log"
)

func main() {
	log.AddFlags()
	log.SetFlags(0)
	log.SetPrefix("gofat: ")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage:
	gofat build   build a fatbin binary
	gofat info    show fatbin binary information
`)
		flag.PrintDefaults()
		os.Exit(2)
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
	}

	cmd, args := flag.Arg(0), flag.Args()[1:]
	switch cmd {
	default:
		fmt.Fprintf(os.Stderr, "unknown command %s\n", cmd)
		flag.Usage()
	case "info":
		info(args)
	case "build":
		build(args)
	}
}

func info(args []string) {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "usage: gofat info binaries...\n")
		os.Exit(2)
	}
	for _, filename := range args {
		f, err := os.Open(filename)
		must(err)
		info, err := f.Stat()
		must(err)
		r, err := fatbin.OpenFile(f, info.Size())
		must(err)
		fmt.Println(filename, r.GOOS()+"/"+r.GOARCH(), info.Size())
		for _, info := range r.List() {
			fmt.Print("\t", info.Goos, "/", info.Goarch, " ", info.Size, "\n")
		}
		must(f.Close())
	}
}

func build(args []string) {
	var (
		flags    = flag.NewFlagSet("build", flag.ExitOnError)
		goarches = flags.String("goarches", "amd64", "list of GOARCH values to build")
		gooses   = flags.String("gooses", "darwin,linux", "list of GOOS values to build")
		out      = flag.String("o", "", "build output path")
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: gofat build [-o output] [packages]\n")
		flags.PrintDefaults()
		os.Exit(2)
	}

	must(flags.Parse(args))
	if *out == "" {
		cmd := exec.Command("go", append([]string{"list"}, flags.Args()...)...)
		cmd.Stderr = os.Stderr
		listout, err := cmd.Output()
		must(err)
		*out = path.Base(string(bytes.TrimSpace(listout)))
	}

	cmd := exec.Command("go", append([]string{"build", "-o", *out}, flags.Args()...)...)
	cmd.Stderr = os.Stderr
	must(cmd.Run())

	f, err := os.OpenFile(*out, os.O_WRONLY|os.O_APPEND, 0777)
	must(err)
	info, err := f.Stat()
	must(err)
	fat := fatbin.NewWriter(f, info.Size(), runtime.GOOS, runtime.GOARCH)

	for _, goarch := range strings.Split(*goarches, ",") {
		for _, goos := range strings.Split(*gooses, ",") {
			if goarch == runtime.GOARCH && goos == runtime.GOOS {
				continue
			}
			outfile, err := ioutil.TempFile("", *out)
			must(err)
			name := outfile.Name()
			outfile.Close()
			cmd := exec.Command("go", "build", "-o", name)
			cmd.Stderr = os.Stderr
			cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch)
			must(cmd.Run())

			outfile, err = os.Open(name)
			must(err)
			w, err := fat.Create(goos, goarch)
			must(err)
			_, err = io.Copy(w, outfile)
			must(err)
			must(os.Remove(name))
			must(outfile.Close())
			log.Print("append ", goos, "/", goarch)
		}
	}
	must(fat.Close())
	must(f.Close())
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
