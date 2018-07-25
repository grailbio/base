// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/grailbio/base/cloud/ec2util"
)

func TestCheckPendingTime(t *testing.T) {
	now := time.Now()
	cases := []struct {
		doc       ec2util.IdentityDocument
		errPrefix string
	}{
		{ec2util.IdentityDocument{PendingTime: now}, ""},
		{ec2util.IdentityDocument{PendingTime: now.Add(-pendingTimeWindow - time.Second)}, "launch time is too old"},
		{ec2util.IdentityDocument{}, "launch time is too old"},
	}

	for _, c := range cases {
		err := checkPendingTime(&c.doc)
		if err != nil && (c.errPrefix == "" || !strings.HasPrefix(err.Error(), c.errPrefix)) {
			t.Errorf("checkPendingTime: got %q, want %q", err, c.errPrefix)
		}
	}
}
