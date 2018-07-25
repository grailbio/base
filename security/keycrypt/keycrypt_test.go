// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package keycrypt

import "testing"

type testSecret struct {
	path  string
	value []byte
}

func (t *testSecret) Get() ([]byte, error) {
	if len(t.value) == 0 {
		return nil, ErrNoSuchSecret
	}
	return t.value, nil
}

func (t *testSecret) Put(v []byte) error {
	t.value = make([]byte, len(v), len(v))
	copy(t.value, v)
	return nil
}

type testKeycrypt struct {
	host    string
	secrets map[string]*testSecret
}

func (t *testKeycrypt) Lookup(name string) Secret {
	if t.secrets == nil {
		t.secrets = make(map[string]*testSecret)
	}
	if t.secrets[name] == nil {
		t.secrets[name] = &testSecret{path: name}
	}
	return t.secrets[name]
}

func TestMarshal(t *testing.T) {
	kc := &testKeycrypt{}
	type data struct {
		A, B, C string
	}
	s := kc.Lookup("foo/bar")
	put := data{"a", "b", "c"}
	if err := PutJSON(s, &put); err != nil {
		t.Fatal(err)
	}
	var get data
	if err := GetJSON(s, &get); err != nil {
		t.Fatal(err)
	}
	if got, want := get, put; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestResolver(t *testing.T) {
	RegisterFunc("test", func(host string) Keycrypt {
		return &testKeycrypt{host: host}
	})
	defer unregister("test")
	s, err := Lookup("test://host/path/to/blah")
	if err != nil {
		t.Fatal(err)
	}
	ts, ok := s.(*testSecret)
	if !ok {
		t.Fatal("bad secret type")
	}
	if want, got := "path/to/blah", ts.path; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
