// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package ticket

import (
	"reflect"
	"testing"

	"github.com/grailbio/base/vcontext"
)

func TestMerge(t *testing.T) {
	ctx := &TicketContext{
		ctx: vcontext.Background(),
	}
	got := mergeOrDie(ctx, &S3Ticket{Endpoint: "xxx"}, &S3Ticket{Bucket: "yyy"})
	want := &S3Ticket{
		Endpoint: "xxx",
		Bucket:   "yyy",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
