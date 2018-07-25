// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package digest

import (
	"context"
	"io"
	"sync"
)

type write struct {
	done chan struct{}
	p    []byte
	off  int64
	next *write
}

// WriterAt can be used to calculate the digest of a file as it is
// being written. It uses back pressure to stall writes when a block
// is missing. WriterAt must be written to sequentially (otherwise a
// deadlock is possible); but it accepts re-writes of past regions so
// that the user can retry failures.
//
// In particular, this matches the semantics for the S3 download
// manager from github.com/aws/aws-sdk-go; thus WriterAt can be used
// to speed up simultaneous download and integrity checking for
// objects stored in S3.
type WriterAt struct {
	ctx          context.Context
	digestWriter Writer
	target       io.WriterAt
	loopOnce     sync.Once
	writes       chan *write
	done         chan struct{}
}

// NewWriterAt creates a new WriterAt. The provided context is used
// to fail pending writes upon cancellation.
func (d Digester) NewWriterAt(ctx context.Context, target io.WriterAt) *WriterAt {
	return &WriterAt{
		ctx:          ctx,
		digestWriter: d.NewWriter(),
		target:       target,
		writes:       make(chan *write),
		done:         make(chan struct{}),
	}
}

func (w *WriterAt) loop() {
	var (
		q   *write
		off int64
	)
	for {
		var (
			wr *write
			ok bool
		)
		select {
		case wr, ok = <-w.writes:
		case <-w.ctx.Done():
		}
		if !ok {
			break
		}
		switch {
		// No overlap: waiting for preceding bytes.
		// Insert ourselves into the waitq.
		case off < wr.off:
			p := &q
			for *p != nil && (*p).off < wr.off {
				p = &(*p).next
			}
			wr.next = *p
			*p = wr
		// No overlap: already written.
		case off >= wr.off+int64(len(wr.p)):
			close(wr.done)
		// Unwritten overlap:
		default:
			wr.next = q
			q = wr
			for q != nil && q.off <= off {
				p := q.p[off-q.off:]
				w.digestWriter.Write(p)
				close(q.done)
				off += int64(len(p))
				q = q.next
			}
		}
	}
	close(w.done)
}

func (w *WriterAt) goloop() { go w.loop() }

// WriteAt implements the WriterAt interface.
func (w *WriterAt) WriteAt(p []byte, off int64) (int, error) {
	w.loopOnce.Do(w.goloop)
	n, err := w.target.WriteAt(p, off)
	if n > 0 {
		wr := &write{done: make(chan struct{}), p: p[:n], off: off}
		select {
		case w.writes <- wr:
		case <-w.ctx.Done():
			return 0, w.ctx.Err()
		}
		select {
		case <-wr.done:
		case <-w.ctx.Done():
			return 0, w.ctx.Err()
		}
	}
	return n, err
}

// Digest returns the digest for the data that has been written. Digest
// waits for writes to flush, and returns underlying context errors.
func (w *WriterAt) Digest() (Digest, error) {
	w.loopOnce.Do(w.goloop)
	close(w.writes)
	select {
	case <-w.done:
	case <-w.ctx.Done():
		return Digest{}, w.ctx.Err()
	}
	return w.digestWriter.Digest(), nil
}
