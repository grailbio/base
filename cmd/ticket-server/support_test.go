package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestStringInSlice(t *testing.T) {
	cases := []struct {
		haystack []string
		match    string
		result   bool
	}{
		{[]string{"a", "b", "c"}, "a", true},
		{[]string{"a", "b", "c"}, "d", false},
	}

	for _, c := range cases {
		got, want := stringInSlice(c.haystack, c.match), c.result
		if got != want {
			t.Errorf("stringInSlice(%+v, %q): got %t, want %t", c.haystack, c.match, got, want)
		}
	}
}

func TestFmap(t *testing.T) {
	cases := []struct {
		stringList []string
		f          func(string) string
		result     []string
	}{
		{[]string{"a", "b", "c"}, strings.ToUpper, []string{"A", "B", "C"}},
	}

	for _, c := range cases {
		got, want := fmap(c.stringList, c.f), c.result
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Map(%+v, ...): got %+v, want %+v", c.stringList, got, want)
		}
	}
}

func TestEmailDomain(t *testing.T) {
	cases := []struct {
		email  string
		domain string
	}{
		{"aeiser@grailbio.com", "grailbio.com"},
		{"aeisergrailbio.com", ""},
		{"aeiser@grail@bio.com", ""},
	}

	for _, c := range cases {
		got, want := emailDomain(c.email), c.domain
		if got != want {
			t.Errorf("emailDomain(%q): got %q, want %q", c.email, got, want)
		}
	}
}
