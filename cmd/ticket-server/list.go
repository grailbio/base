package main

import (
	"regexp"
	"sort"

	"github.com/grailbio/base/common/log"
	"v.io/v23/context"
	"v.io/v23/rpc"
)

type list struct{}

func newList(ctx *context.T) *list {
	return &list{}
}

func (l *list) List(ctx *context.T, call rpc.ServerCall) ([]string, error) {
	log.Info(ctx, "list request", "endpoint", "get", "blessing", call.Security().RemoteBlessings(), "ticket", call.Suffix())
	var result []string
	ignored := regexp.MustCompile("blesser/*|list")
	for t, e := range d.registry {
		if ignore := ignored.MatchString(t); !ignore {
			if err := e.auth.Authorize(ctx, call.Security()); err == nil {
				result = append(result, t)
			}
		}
	}
	sort.Strings(result)
	return result, nil
}
