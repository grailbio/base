// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package digest

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// Reader can be used to calculate the digest of a file as it is being
// read. It uses back pressure to stall reads when a block is missing.
// This can cause deadlock if the application doesn't retry immediately.
//
// The s3manager uploader differentiates between two kinds of readers to
// improve upload performance: Simple Readers and "ReaderSeekers" for
// performance. This implementation creates either a simpleReaderAt or a
// readerAtSeeker depending on the underlying ReaderAt.
//
// Expects the reads to be complete and non-overlapping.
type Reader interface {
	io.Reader
	Digest() (Digest, error)
}

type readerWrap struct {
	mu           sync.Mutex // GUARDS reader.
	err          error
	digestWriter Writer
	source       io.Reader
}

// Digest returns the digest for the data that has been read.
func (r *readerWrap) Digest() (Digest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.err != nil {
		return Digest{}, r.err
	}

	return r.digestWriter.Digest(), nil
}

// Read implements the io.Reader interface. It reads data from the file
// and places it in p, returning the number of bytes placed in the slice as
// well as any error.
func (r *readerWrap) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n, err := r.source.Read(p)
	r.err = err

	if r.err != nil {
		return n, r.err
	}

	q := p[:n]
	// todo(ysiato, schandra, pknudsgaard) this looks like another intentional no-error-check like digest.go:407
	r.digestWriter.Write(q)

	return n, r.err
}

type readerAtSeeker interface {
	io.ReaderAt
	io.ReadSeeker
}

type readerAtSeekerWrap struct {
	mu           sync.Mutex
	cond         *sync.Cond
	pending      int64
	err          error
	current      int64
	digestWriter Writer
	source       readerAtSeeker
}

// Read is present to fulfill the io.Reader API, but should not be called.
func (ras *readerAtSeekerWrap) Read(p []byte) (n int, err error) {
	panic("Read should not be called on ReaderAt")
}

// ReadAt implements the ReaderAt interface.
func (ras *readerAtSeekerWrap) ReadAt(p []byte, off int64) (int, error) {
	// pending should be incremented, but concurrency for the source.ReadAt
	// should be maintained. Using atomic means that we don't have to
	// acquire/release/read/acquire/release.
	for {
		n := atomic.LoadInt64(&ras.pending)
		if n < 0 {
			panic("digest already called")
		}
		if atomic.CompareAndSwapInt64(&ras.pending, n, n+1) {
			break
		}
	}
	defer atomic.AddInt64(&ras.pending, -1)

	n, err := ras.source.ReadAt(p, off)

	ras.mu.Lock()
	defer ras.mu.Unlock()

	if ras.err != nil {
		return 0, ras.err
	}

	ras.err = err

	for ras.current != off && ras.err == nil {
		ras.cond.Wait()
	}

	if ras.err != nil {
		return 0, ras.err
	}

	q := p[:n]
	ras.digestWriter.Write(q)

	ras.current += int64(n)
	ras.cond.Broadcast()

	return n, ras.err
}

func (ras *readerAtSeekerWrap) Seek(offset int64, whence int) (int64, error) {
	return ras.source.Seek(offset, whence)
}

// Digest returns the digest for the data. Digest cannot be called with pending
// reads.
func (ras *readerAtSeekerWrap) Digest() (Digest, error) {
	ras.mu.Lock()
	defer ras.mu.Unlock()

	for {
		n := atomic.LoadInt64(&ras.pending)
		if n > 0 {
			panic(fmt.Sprintf("Digest() called before all writes have completed, %d pending", ras.pending))
		}
		if n < 0 || atomic.CompareAndSwapInt64(&ras.pending, n, -1) {
			break
		}
	}

	if ras.err != nil {
		return Digest{}, ras.err
	}

	return ras.digestWriter.Digest(), nil
}

// NewReader creates a new WriterAt.
func (d Digester) NewReader(source io.Reader) Reader {
	ras, ok := source.(readerAtSeeker)
	if ok {
		result := &readerAtSeekerWrap{
			digestWriter: d.NewWriter(),
			source:       ras,
		}
		result.cond = sync.NewCond(&result.mu)

		return result
	}

	result := &readerWrap{
		digestWriter: d.NewWriter(),
		source:       source,
	}

	return result
}
