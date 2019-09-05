// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build darwin dragonfly freebsd linux openbsd solaris netbsd

// Package oom contains a single function to trigger Linux kernel OOMs.
package oom

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// Do attempts to OOM the process by allocating up
// to the provided number of bytes. Do never returns.
func Do(size int) {
	log.Print("oom: allocating ", size, " bytes")
	var (
		prot = unix.PROT_READ | unix.PROT_WRITE
		flag = unix.MAP_PRIVATE | unix.MAP_ANON | unix.MAP_NORESERVE
	)
	b, err := unix.Mmap(-1, 0, size, prot, flag)
	if err != nil {
		log.Fatal(err)
	}
	stride := os.Getpagesize()
	// Touch each page so that the process gradually allocates
	//  more and more memory.
	for i := 0; i < size; i += stride {
		b[i] = 1
	}
	log.Fatal("failed to OOM process")
}

// Try attempts to OOM based on the available physical memory and
// default overcommit heuristics. Try never returns.
func Try() {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		log.Fatal(err)
	}
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		if len(fields) < 2 {
			continue
		}
		if fields[0] != "MemTotal:" {
			continue
		}
		if fields[2] != "kB" {
			log.Fatalf("expected kilobytes, got %s", fields[2])
		}
		kb, err := strconv.ParseInt(fields[1], 0, 64)
		if err != nil {
			log.Fatalf("parsing %q: %v", fields[2], err)
		}
		Do(int(kb << 10))
	}
	if err := scan.Err(); err != nil {
		log.Fatal(err)
	}
	log.Fatal("MemTotal not found in /proc/meminfo")
}
