package main

import (
	"testing"

	"github.com/grailbio/base/security/ticket"
)

func TestCheckControls(t *testing.T) {
	cases := []struct {
		s          service
		args       map[string]string
		wantOk     bool
		wantErrMsg string
	}{
		{
			service{},
			map[string]string{},
			true,
			"",
		},
		{
			service{
				controls: map[ticket.Control]bool{
					ticket.ControlRationale: true,
				},
			},
			map[string]string{
				"Rationale": "rationale",
			},
			true,
			"",
		},
		{
			service{
				controls: map[ticket.Control]bool{
					ticket.ControlRationale: true,
				},
			},
			map[string]string{},
			false,
			"missing required argument: Rationale",
		},
	}
	for _, c := range cases {
		ok, err := c.s.checkControls(nil, nil, c.args)
		if ok != c.wantOk {
			t.Errorf("unexpected ok value: got: %t, want: %t", ok, c.wantOk)
		}
		if c.wantErrMsg != "" && err.Error() != c.wantErrMsg {
			t.Errorf("unexpected err value: got: %s, want: %s", err.Error(), c.wantErrMsg)
		}
	}

}
