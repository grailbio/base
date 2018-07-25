// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package deprecated_test

import "github.com/golang/protobuf/proto"

// recordioUnmarshal is intended for use with recordio Scanners.
func recordioUnmarshal(b []byte, v interface{}) error {
	return proto.Unmarshal(b, v.(proto.Message))
}

// recordioMarshal is intended for use with recordio Writers.
func recordioMarshal(scratch []byte, v interface{}) ([]byte, error) {
	return proto.Marshal(v.(proto.Message))
}
