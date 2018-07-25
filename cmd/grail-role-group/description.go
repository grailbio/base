package main

import (
	"fmt"
	"strings"
)

// description generates a standard description for the group. It assumes the
// group name for <account>/<role> is <account>-<role>-<type>@grailbio.com.
func description(group string) string {
	if strings.HasSuffix(group, "-aws-role@grailbio.com") {
		v := strings.SplitN(strings.TrimSuffix(group, "-aws-role@grailbio.com"), "-", 2)
		if len(v) != 2 {
			return ""
		}
		return fmt.Sprintf("Please request access to this group if you need access to the %s/%s role account.", v[0], v[1])
	} else if strings.HasSuffix(group, "-ticket@grailbio.com") {
		v := strings.TrimSuffix(group, "-ticket@grailbio.com")
		return fmt.Sprintf("Please request access to this group if you need access to the ticket %s.", v)
	} else {
		return ""
	}
}
