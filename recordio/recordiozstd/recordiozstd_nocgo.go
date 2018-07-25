// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build !cgo

// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package recordiozstd

import (
	"errors"
	"sync"

	"github.com/grailbio/base/recordio"
)

var once sync.Once

// Init registers a dummy implementation for recordio zstd compression.
// The registered transformers always return an error.
func Init() {
	once.Do(func() {
		recordio.RegisterTransformer(
			Name,
			func(config string) (recordio.TransformFunc, error) {
				return nil, errors.New("zstd not supported on non-cgo platforms")
			},
			func(string) (recordio.TransformFunc, error) {
				return nil, errors.New("zstd not supported on non-cgo platforms")
			})
	})
}
