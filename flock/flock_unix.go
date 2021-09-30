//go:build !windows
// +build !windows

package flock

import (
	"context"
	"sync"
	"syscall"

	"github.com/grailbio/base/log"
)

type unixlock struct {
	name string
	fd   int
	mu   sync.Mutex
}

// New creates an object that locks the given path.
func PlatformSpecificLock(path string) FileLock {
	return &unixlock{name: path}
}

// Lock locks the file. Iff Lock() returns nil, the caller must call Unlock()
// later.
func (f *unixlock) Lock(ctx context.Context) (err error) {
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
	reqCh <- f.doLock
	select {
	case <-ctx.Done():
		reqCh <- f.doUnlock
		err = ctx.Err()
	case err = <-doneCh:
	}
	close(reqCh)
	return err
}

// Unlock unlocks the file.
func (f *unixlock) Unlock() error {
	return f.doUnlock()
}

func (f *unixlock) doLock() error {
	f.mu.Lock() // Serialize the lock within one process.

	var err error
	f.fd, err = syscall.Open(f.name, syscall.O_CREAT|syscall.O_RDWR, 0777)
	if err != nil {
		f.mu.Unlock()
		return err
	}
	err = syscall.Flock(f.fd, syscall.LOCK_EX|syscall.LOCK_NB)
	for err == syscall.EWOULDBLOCK || err == syscall.EAGAIN {
		log.Printf("waiting for lock %s", f.name)
		err = syscall.Flock(f.fd, syscall.LOCK_EX)
	}
	if err != nil {
		f.mu.Unlock()
	}
	return err
}

func (f *unixlock) doUnlock() error {
	err := syscall.Flock(f.fd, syscall.LOCK_UN)
	if err := syscall.Close(f.fd); err != nil {
		log.Error.Printf("close %s: %v", f.name, err)
	}
	f.mu.Unlock()
	return err
}
