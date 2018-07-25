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

	"github.com/grailbio/base/embedbin"
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
	gofat embed   build an embedbin binary
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
	case "embed":
		embed(args)
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
		out      = flags.String("o", "", "build output path")
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

func embed(args []string) {
	var (
		flags = flag.NewFlagSet("embed", flag.ExitOnError)
		out   = flags.String("o", "", "build output path")
	)
	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: gofat embed [-o output] [name1:path1 [name2:path2 ...]]\n")
		flags.PrintDefaults()
		os.Exit(2)
	}

	must(flags.Parse(args))
	args = flags.Args()
	if len(args) == 0 {
		log.Fatal("missing path to input binary")
	}
	inputPath, args := args[0], args[1:]

	paths := map[string]string{}
	var names []string
	for _, arg := range args {
		parts := strings.SplitN(arg, ":", 2)
		if len(parts) != 2 {
			log.Fatalf("malformed argument: %s", arg)
		}
		name, path := parts[0], parts[1]
		if _, ok := paths[name]; ok {
			log.Fatalf("duplicate name: %s", name)
		}
		paths[name] = path
		names = append(names, name)
	}

	var outF *os.File
	var err error
	if *out != "" {
		outF, err = os.OpenFile(*out, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
		must(err)
		var inF *os.File
		inF, err = os.Open(inputPath)
		must(err)
		_, err = io.Copy(outF, inF)
		must(err)
		must(inF.Close())
	} else {
		outF, err = os.OpenFile(inputPath, os.O_RDWR|os.O_APPEND, 0777)
		must(err)
	}

	ew, err := embedbin.NewFileWriter(outF)
	must(err)
	for _, name := range names {
		embedF, err := os.Open(paths[name])
		must(err)
		embedW, err := ew.Create(name)
		must(err)
		_, err = io.Copy(embedW, embedF)
		must(err)
		must(embedF.Close())
	}
	must(ew.Close())
	must(outF.Close())
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
