//go:build windows
// +build windows

package flock

import (
	"context"
	"io/fs"
	"sync"

	"golang.org/x/sys/windows"
)

// The windows implementation of a lock is based on Golang's
// internal implementation at:
// https://cs.opensource.google/go/go/+/master:src/cmd/go/internal/lockedfile/lockedfile.go
type winlock struct {
	path     string
	fdHandle windows.Handle
	mu       sync.Mutex
}

func PlatformSpecificLock(path string) FileLock {
	return &winlock{path, 0, sync.Mutex{}}
}

func (w *winlock) Unlock() error {
	defer w.mu.Unlock()
	ol := new(windows.Overlapped)
	err := windows.UnlockFileEx(w.fdHandle, reserved, allBytes, allBytes, ol)
	if err != nil {
		return &fs.PathError{
			Op:   "Unlock",
			Path: w.path,
			Err:  err,
		}
	}
	return nil
}

func (w *winlock) Lock(ctx context.Context) (err error) {
	reqCh := make(chan func() error, 2)
	doneCh := make(chan error)
	go func() {
		var err error
		for req := range reqCh {
			if err == nil {
				err = req()
			}
			doneCh <- err
		}
	}()
	reqCh <- w.doLock
	select {
	case <-ctx.Done():
		reqCh <- w.Unlock
		err = ctx.Err()
	case err = <-doneCh:
	}
	close(reqCh)
	return err
}

func (w *winlock) doLock() (err error) {
	// Per https://golang.org/issue/19098, “Programs currently expect the Fd
	// method to return a handle that uses ordinary synchronous I/O.”
	// However, LockFileEx still requires an OVERLAPPED structure,
	// which contains the file offset of the beginning of the lock range.
	// We want to lock the entire file, so we leave the offset as zero.

	w.mu.Lock()
	fdHandle, err := windows.Open(w.path, windows.O_CREAT|windows.O_RDWR, 0777)

	if err != nil {
		w.mu.Unlock()
		return err
	}
	w.fdHandle = fdHandle
	ol := new(windows.Overlapped)

	err = windows.LockFileEx(w.fdHandle, uint32(writeLock), reserved, allBytes, allBytes, ol)
	if err != nil {
		w.mu.Unlock()
		return &fs.PathError{
			Op:   writeLock.String(),
			Path: w.path,
			Err:  err,
		}
	}

	return nil
}

type lockType uint32

const (
	readLock  lockType = 0
	writeLock lockType = windows.LOCKFILE_EXCLUSIVE_LOCK
)

func (lt lockType) String() string {
	switch lt {
	case readLock:
		return "readLock"
	case writeLock:
		return "writeLock"
	default:
		return "unknown lock type"
	}
}

const (
	reserved = 0
	allBytes = ^uint32(0)
)
