// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package fatbin implements a simple fat binary format, and provides
// facilities for creating fat binaries and accessing its variants.
//
// A fatbin binary is a base binary with a zip archive appended,
// containing copies of the same binary targeted to different
// GOOS/GOARCH combinations. The zip archive contains one entry for
// each supported architecture and operating system combination.
// At the end of a fatbin image is a footer, storing the offset of the
// zip archive as well as a magic constant used to identify fatbin
// images:
//
//	[8]offset[4]magic[8]checksum
//
// The checksum is a 64-bit xxhash checksum of the offset and
// magic fields. The magic value is 0x5758ba2c.
package fatbin

import (
	"archive/zip"
	"debug/elf"
	"debug/macho"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/grailbio/base/log"
)

var (
	selfOnce sync.Once
	self     *Reader
	selfErr  error
)

var (
	// ErrNoSuchImage is returned when the fatbin does not contain an
	// image for the requested GOOS/GOARCH combination.
	ErrNoSuchImage = errors.New("image does not exist")
	// ErrCorruptedImage is returned when the fatbin image has been
	// corrupted.
	ErrCorruptedImage = errors.New("corrupted fatbin image")
)

// Info provides information for an embedded binary.
type Info struct {
	Goos, Goarch string
	Size         int64
}

func (info Info) String() string {
	return fmt.Sprintf("%s/%s: %d", info.Goos, info.Goarch, info.Size)
}

// Reader reads images from a fatbin.
type Reader struct {
	self         io.ReaderAt
	goos, goarch string
	offset       int64

	z *zip.Reader
}

// Self reads the currently executing binary image as a fatbin and
// returns a reader to it.
func Self() (*Reader, error) {
	selfOnce.Do(func() {
		filename, err := os.Executable()
		if err != nil {
			selfErr = err
			return
		}
		f, err := os.Open(filename)
		if err != nil {
			selfErr = err
			return
		}
		info, err := f.Stat()
		if err != nil {
			selfErr = err
			return
		}
		_, _, offset, err := Sniff(f, info.Size())
		if err != nil {
			selfErr = err
			return
		}
		self, selfErr = NewReader(f, offset, info.Size(), runtime.GOOS, runtime.GOARCH)
	})
	return self, selfErr
}

// OpenFile parses the provided ReaderAt with the provided size. The
// file's contents is parsed to determine the offset of the fatbin's
// archive. OpenFile returns an error if the file is not a fatbin.
func OpenFile(r io.ReaderAt, size int64) (*Reader, error) {
	goos, goarch, offset, err := Sniff(r, size)
	if err != nil {
		return nil, err
	}
	return NewReader(r, offset, size, goos, goarch)
}

// NewReader returns a new fatbin reader from the provided reader.
// The offset should be the offset of the fatbin archive; size is the
// total file size. The provided goos and goarch are that of the base
// binary.
func NewReader(r io.ReaderAt, offset, size int64, goos, goarch string) (*Reader, error) {
	rd := &Reader{
		self:   io.NewSectionReader(r, 0, offset),
		goos:   goos,
		goarch: goarch,
		offset: offset,
	}
	if offset == size {
		return rd, nil
	}
	var err error
	rd.z, err = zip.NewReader(io.NewSectionReader(r, offset, size-offset), size-offset)
	if err != nil {
		return nil, err
	}
	return rd, nil
}

// GOOS returns the base binary GOOS.
func (r *Reader) GOOS() string { return r.goos }

// GOARCH returns the base binary GOARCH.
func (r *Reader) GOARCH() string { return r.goarch }

// List returns information about embedded binary images.
func (r *Reader) List() []Info {
	infos := make([]Info, len(r.z.File))
	for i, f := range r.z.File {
		elems := strings.SplitN(f.Name, "/", 2)
		if len(elems) != 2 {
			log.Error.Printf("invalid fatbin: found name %s", f.Name)
			continue
		}
		infos[i] = Info{
			Goos:   elems[0],
			Goarch: elems[1],
			Size:   int64(f.UncompressedSize64),
		}
	}
	return infos
}

// Open returns a ReadCloser from which the binary with the provided
// goos and goarch can be read. Open returns ErrNoSuchImage if the
// fatbin does not contain an image for the requested goos and
// goarch.
func (r *Reader) Open(goos, goarch string) (io.ReadCloser, error) {
	if goos == r.goos && goarch == r.goarch {
		sr := io.NewSectionReader(r.self, 0, 1<<63-1)
		return ioutil.NopCloser(sr), nil
	}

	if r.z == nil {
		return nil, ErrNoSuchImage
	}

	look := goos + "/" + goarch
	for _, f := range r.z.File {
		if f.Name == look {
			return f.Open()
		}
	}
	return nil, ErrNoSuchImage
}

// Stat returns the information for the image identified by the
// provided GOOS and GOARCH. It returns a boolean indicating
// whether the requested image was found.
func (r *Reader) Stat(goos, goarch string) (info Info, ok bool) {
	info.Goos = goos
	info.Goarch = goarch
	if goos == r.goos && goarch == r.goarch {
		info.Size = r.offset
		ok = true
		return
	}
	look := goos + "/" + goarch
	for _, f := range r.z.File {
		if f.Name == look {
			info.Size = int64(f.UncompressedSize64)
			ok = true
			return
		}
	}
	return
}

func sectionEndAligned(s *elf.Section) int64 {
	return int64(((s.Offset + s.FileSize) + (s.Addralign - 1)) & -s.Addralign)
}

// Sniff sniffs a binary's goos, goarch, and fatbin offset. Sniff returns errors
// returned by the provided reader, or ErrCorruptedImage if the binary is identified
// as a fatbin image with a checksum mismatch.
func Sniff(r io.ReaderAt, size int64) (goos, goarch string, offset int64, err error) {
	for _, s := range sniffers {
		var ok bool
		goos, goarch, ok = s(r)
		if ok {
			break
			return
		}
	}
	if goos == "" {
		goos = "unknown"
	}
	if goarch == "" {
		goarch = "unknown"
	}
	offset, err = readFooter(r, size)
	if err == errNoFooter {
		err = nil
		offset = size
	}
	return
}

type sniffer func(r io.ReaderAt) (goos, goarch string, ok bool)

var sniffers = []sniffer{sniffElf, sniffMacho}

func sniffElf(r io.ReaderAt) (goos, goarch string, ok bool) {
	file, err := elf.NewFile(r)
	if err != nil {
		return
	}
	ok = true
	switch file.OSABI {
	default:
		goos = "unknown"
	case elf.ELFOSABI_NONE, elf.ELFOSABI_LINUX:
		goos = "linux"
	case elf.ELFOSABI_NETBSD:
		goos = "netbsd"
	case elf.ELFOSABI_OPENBSD:
		goos = "openbsd"
	}
	switch file.Machine {
	default:
		goarch = "unknown"
	case elf.EM_386:
		goarch = "386"
	case elf.EM_X86_64:
		goarch = "amd64"
	case elf.EM_ARM:
		goarch = "arm"
	case elf.EM_AARCH64:
		goarch = "arm64"
	}
	return
}

func sniffMacho(r io.ReaderAt) (goos, goarch string, ok bool) {
	file, err := macho.NewFile(r)
	if err != nil {
		return
	}
	ok = true
	// We assume mach-o is only used in Darwin. This is not exposed
	// by the mach-o files.
	goos = "darwin"
	switch file.Cpu {
	default:
		goarch = "unknown"
	case macho.Cpu386:
		goarch = "386"
	case macho.CpuAmd64:
		goarch = "amd64"
	case macho.CpuArm:
		goarch = "arm"
	case macho.CpuArm64:
		goarch = "arm64"
	case macho.CpuPpc:
		goarch = "ppc"
	case macho.CpuPpc64:
		goarch = "ppc64"
	}
	return
}
