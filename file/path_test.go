package file

import (
	"testing"
)

func TestJoin(t *testing.T) {
	tests := []struct {
		elems []string
		want  string
	}{
		{
			[]string{"foo/"}, // trailing separator removed from first element.
			"foo",
		},
		{
			[]string{"foo", "bar"}, // join adds separator
			"foo/bar",
		},
		{
			[]string{"foo", "bar/"}, // trailing separator removed from second element.
			"foo/bar",
		},
		{
			[]string{"/foo", "bar"}, // leading separator is retained in first element.
			"/foo/bar",
		},
		{
			[]string{"foo/", "bar"}, // trailing separator removed before join.
			"foo/bar",
		},
		{
			[]string{"foo/", "/bar"}, // all separators removed before join.
			"foo/bar",
		},
		{
			[]string{"foo/", "/bar", "baz"}, // all separators removed before join.
			"foo/bar/baz",
		},
		{
			[]string{"foo/", "bar", "/baz"}, // all separators removed before join.
			"foo/bar/baz",
		},
		{
			[]string{"http://foo/", "/bar"}, // separators inside the element are retained.
			"http://foo/bar",
		},
	}

	for i, test := range tests {
		if got, want := Join(test.elems...), test.want; got != want {
			t.Errorf("test %d: got %q, want %q", i, got, want)
		}
	}
}
