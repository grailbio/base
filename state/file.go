// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package state implements atomic file-based state management
// with support for advisory locking.
package state

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// ErrNoState is returned when attempting to read a nonexistent state.
var ErrNoState = errors.New("no state exists")

// File implements file-based state management with support for
// advisory locking. It is also safe to use concurrently within a
// process.
type File struct {
	mu     sync.Mutex
	prefix string
	lockfd int
}

// New creates and returns a new State at the given prefix. The
// following files are stored:
// 	- {prefix}.json: the current state
// 	- {prefix}.lock: the POSIX lock file
// 	- {prefix}.bak: the previous state
func Open(prefix string) (*File, error) {
	f := &File{prefix: prefix}
	os.MkdirAll(filepath.Dir(prefix), 0777) // best-effort
	var err error
	f.lockfd, err = syscall.Open(prefix+".lock", syscall.O_CREAT|syscall.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Marshal opens a State, marshals v into it, and then closes it.
func Marshal(prefix string, v interface{}) error {
	file, err := Open(prefix)
	if err != nil {
		return err
	}
	err = file.Marshal(v)
	file.Close()
	return err
}

// Unmarshal opens a State, unmarshals it into v, and then closes it.
func Unmarshal(prefix string, v interface{}) error {
	file, err := Open(prefix)
	if err != nil {
		return err
	}
	err = file.Unmarshal(v)
	file.Close()
	return err
}

// Lock locks the state, both inside of the process and outside. Lock
// relies on POSIX flock, which may not be available on all
// filesystems, notably NFS and SMB.
func (f *File) Lock() error {
	f.mu.Lock()
	if err := syscall.Flock(f.lockfd, syscall.LOCK_EX); err != nil {
		f.mu.Unlock()
		return err
	}
	return nil
}

// Unlock unlocks the state.
func (f *File) Unlock() error {
	if err := syscall.Flock(f.lockfd, syscall.LOCK_UN); err != nil {
		return err
	}
	f.mu.Unlock()
	return nil
}

// LockLocal locks local access to state.
func (f *File) LockLocal() {
	f.mu.Lock()
}

// UnlockLocal unlocks local access to state.
func (f *File) UnlockLocal() {
	f.mu.Unlock()
}

// Marshal atomically stores the JSON-encoded representation of v to
// the current state. It is only stored when Marshal returns a nil
// error.
func (f *File) Marshal(v interface{}) error {
	w, err := ioutil.TempFile(filepath.Dir(f.prefix), filepath.Base(f.prefix)+".write")
	if err != nil {
		return err
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	os.Remove(f.prefix + ".bak")
	os.Link(f.prefix+".json", f.prefix+".bak")
	return os.Rename(w.Name(), f.prefix+".json")
}

// Unmarshal decodes the current state into v. Unmarshal returns
// ErrNoState if no state is stored.
func (f *File) Unmarshal(v interface{}) error {
	w, err := os.Open(f.prefix + ".json")
	if os.IsNotExist(err) {
		return ErrNoState
	} else if err != nil {
		return err
	}
	defer w.Close()
	return json.NewDecoder(w).Decode(v)
}

// Close releases resources associated with this State instance.
func (f *File) Close() error {
	return syscall.Close(f.lockfd)
}
