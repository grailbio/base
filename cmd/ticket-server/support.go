// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"strings"
)

// Return true if a string matches a value in a list
func stringInSlice(haystack []string, needle string) bool {
	for _, s := range haystack {
		if needle == s {
			return true
		}
	}
	return false
}

// Return a new list after applying a function to the provided list
func fmap(stringList []string, f func(string) string) []string {
	resultList := make([]string, len(stringList))
	for i, v := range stringList {
		resultList[i] = f(v)
	}
	return resultList
}

// Returns the domain part of an email, or "" if it did not split correctly
func emailDomain(email string) string {
	components := strings.Split(email, "@")
	// Email should have 2 parts.
	if len(components) != 2 {
		return ""
	} else {
		return components[1] // domain part
	}
}
