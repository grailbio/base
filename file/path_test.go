package file_test

import (
	"fmt"
	"testing"

	"github.com/grailbio/base/file"
	"github.com/grailbio/testutil/expect"
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
		{
			[]string{"s3://", "bar"},
			"s3://bar",
		},
		{
			[]string{"s3://", "/bar"},
			"s3://bar",
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			expect.EQ(t, file.Join(test.elems...), test.want)
		})
	}
}
