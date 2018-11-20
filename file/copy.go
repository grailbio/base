// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package file

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/grailbio/base/errors"
)

const (
	defaultBufferSize = 32 * 1024
)

// Copy is a context-aware version of io.Copy.
// Note that canceling the context doesn't undo the effects of a partial copy.
func Copy(ctx context.Context, dst io.Writer, src io.Reader) (written int64, err error) {
	var stop int32
	done := make(chan struct{})
	go func() {
		buf := make([]byte, defaultBufferSize)
		defer close(done)
		for {
			nr, er := src.Read(buf)
			if nr > 0 {
				nw, ew := dst.Write(buf[0:nr])
				if nw > 0 {
					written += int64(nw)
				}
				if ew != nil {
					err = errors.E("file.Copy", ew)
					return
				}
				if nr != nw {
					err = errors.E("file.Copy", io.ErrShortWrite)
					return
				}
			}
			if er != nil {
				if er != io.EOF {
					err = errors.E("file.Copy", er)
				}
				return
			}
			if atomic.LoadInt32(&stop) != 0 {
				break
			}
		}
	}()
	select {
	case <-ctx.Done():
		err = ctx.Err()
		// Stop the copy goroutine.
		atomic.StoreInt32(&stop, 1)
		// Wait until its done.
		<-done
	case <-done:
	}
	return written, err
}
