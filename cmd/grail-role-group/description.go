package main

import (
	"fmt"
	"strings"
)

// description generates a standard description for the group. It assumes the
// group name for <account>/<role> is <account>-<role>-aws-role@grailbio.com.
func description(group string) string {
	v := strings.SplitN(strings.TrimSuffix(group, groupSuffix), "-", 2)
	if len(v) != 2 {
		return ""
	}
	return fmt.Sprintf("Please request access to this group if you need access to the %s/%s role account.", v[0], v[1])
}
