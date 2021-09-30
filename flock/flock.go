// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package flock implements a simple POSIX file-based advisory lock.
package flock

import (
	"context"
)

type FileLock interface {
	Lock(ctx context.Context) (err error)
	Unlock() error
}

func New(path string) FileLock {
	return NewLockPlatformSpecific(path)
}
