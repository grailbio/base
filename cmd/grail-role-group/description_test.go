package main

import "testing"

func TestDescription(t *testing.T) {
	cases := []struct {
		group string
		want  string
	}{
		{
			"eng-dev-aws-role@grailbio.com",
			"Please request access to this group if you need access to the eng/dev role account.",
		},
		{
			"vendor-procured-samples-ticket@grailbio.com",
			"Please request access to this group if you need access to the ticket vendor-procured-samples.",
		},
		{"eng", ""},
		{"", ""},
	}

	for _, c := range cases {
		got := description(c.group)
		if got != c.want {
			t.Errorf("description(%q): got %q, want %q", c.group, got, c.want)
		}
	}
}
