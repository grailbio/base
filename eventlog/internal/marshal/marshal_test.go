// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package marshal

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestMarshal verifies that Marshal behaves properly in both success and
// failure cases. For success cases, it roundtrips the marshaled string and
// verifies the result. For failure cases, it checks for expected error
// messages.
func TestMarshal(t *testing.T) {
	for _, c := range []struct {
		name       string
		fieldPairs []interface{}
		// errNeedle is "" if we expect no error. Otherwise, it is a string that
		// we expect to see in the resulting err.Error().
		errNeedle string
	}{
		{
			"no fields",
			[]interface{}{},
			"",
		},
		{
			"simple",
			[]interface{}{"k0", "v0"},
			"",
		},
		{
			"mixed value types",
			// Numeric types turn into float64s in JSON.
			[]interface{}{"k0", "v0", "k1", float64(1), "k2", true},
			"",
		},
		{
			"odd field pairs",
			[]interface{}{"k0", "v0", "k1"},
			"even",
		},
		{
			"non-string key",
			[]interface{}{0, "v0"},
			"string",
		},
		{
			"duplicate keys",
			[]interface{}{"k0", "v0", "k0", "v1"},
			"duplicate",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			marshalOK := c.errNeedle == ""
			s, err := Marshal(c.name, c.fieldPairs)
			if got, want := err == nil, marshalOK; got != want {
				t.Fatalf("got %v, want %v", got, want)
			}
			if !marshalOK {
				if !strings.Contains(err.Error(), c.errNeedle) {
					t.Errorf("error %q does not contain expected substring %q", err.Error(), c.errNeedle)
				}
				return
			}
			var m map[string]interface{}
			err = json.Unmarshal([]byte(s), &m)
			if err != nil {
				t.Fatalf("unmarshaling failed: %v", err)
			}
			// The +1 is for the eventType.
			if got, want := len(m), (len(c.fieldPairs)/2)+1; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			typ, ok := m[eventTypeFieldKey]
			if ok {
				if got, want := typ, c.name; got != want {
					t.Errorf("got %v, want %v", got, want)
				}
			} else {
				t.Errorf("eventType field not marshaled")
			}
			for i := 0; i < len(c.fieldPairs); i++ {
				key := c.fieldPairs[i].(string)
				i++
				value := c.fieldPairs[i]
				mvalue, ok := m[key]
				if !ok {
					t.Errorf("field with key %q not marshaled", key)
					continue
				}
				if got, want := mvalue, value; got != want {
					t.Errorf("got %v(%T), want %v(%T)", got, got, want, want)
				}
			}
		})
	}
}
