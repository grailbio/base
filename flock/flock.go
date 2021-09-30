// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package flock implements a simple POSIX file-based advisory lock.
package flock

import (
	"context"
)

type T struct {
	impl FileLock
}

func New(path string) *T {
	return &T{PlatformSpecificLock(path)}
}

func (lockType *T) Lock(ctx context.Context) (err error) {
	return lockType.impl.Lock(ctx)
}

func (lockType *T) Unlock() error {
	return lockType.impl.Unlock()
}

type FileLock interface {
	Lock(ctx context.Context) (err error)
	Unlock() error
}
