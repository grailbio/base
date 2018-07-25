// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package deprecated_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/grailbio/base/recordio/deprecated"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/expect"
)

type TestPB struct {
	Message string `protobuf:"bytes,1,opt,name=message" json:"message,omitempty"`
}

func (m *TestPB) Reset()         { *m = TestPB{} }
func (m *TestPB) String() string { return proto.CompactTextString(m) }
func (*TestPB) ProtoMessage()    {}

func (m *TestPB) GetName() string {
	return m.Message
}

func getTestMessages() []proto.Message {
	return []proto.Message{
		&TestPB{"hello"},
		&TestPB{"goodbye"},
	}
}

func TestProto(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	wropts := deprecated.LegacyWriterOpts{Marshal: recordioMarshal}

	writer := deprecated.NewLegacyWriter(buf, wropts)

	testMessages := getTestMessages()
	for _, pb := range testMessages {
		n, err := writer.Marshal(pb)
		assert.NoError(t, err)
		tmp := proto.NewBuffer(nil)
		assert.NoError(t, tmp.Marshal(pb))
		if got, want := n, len(tmp.Bytes()); got != want {
			t.Fatalf("%v: got %v, want %v", pb.String(), got, want)
		}
	}

	scopts := deprecated.LegacyScannerOpts{Unmarshal: recordioUnmarshal}
	scanner := deprecated.NewLegacyScanner(buf, scopts)

	nMessages := 0
	for scanner.Scan() {
		pb := &TestPB{}
		if err := scanner.Unmarshal(pb); err != nil {
			t.Errorf("unmarshal: %v", err)
		}
		if got, want := pb.String(), testMessages[nMessages].String(); got != want {
			t.Fatalf("%v: got %v, want %v", nMessages, got, want)
		}

		nMessages++
	}
	if err := scanner.Err(); err != nil {
		t.Errorf("unexpected scanner error: %v", err)
	}

	if got, want := nMessages, len(testMessages); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestMarshalErrors(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	writer := deprecated.NewLegacyWriter(buf, deprecated.LegacyWriterOpts{})
	_, err := writer.Marshal(&TestPB{"error"})
	expect.HasSubstr(t, err, "Marshal function not configured")

	wropts := deprecated.LegacyWriterOpts{Marshal: func(scratch []byte, v interface{}) ([]byte, error) {
		return nil, fmt.Errorf("oh a marshaling error")
	}}

	writer = deprecated.NewLegacyWriter(buf, wropts)
	_, err = writer.Marshal(&TestPB{"error"})
	expect.HasSubstr(t, err, "oh a marshaling error")

	wropts.Marshal = recordioMarshal
	writer = deprecated.NewLegacyWriter(buf, wropts)
	_, err = writer.Marshal(&TestPB{"error"})
	if err != nil {
		t.Fatal(err)
	}
	scanner := deprecated.NewLegacyScanner(buf, deprecated.LegacyScannerOpts{})
	scanner.Scan()
	pb := &TestPB{}
	err = scanner.Unmarshal(pb)
	expect.HasSubstr(t, err, "Unmarshal function not configured")

	scopts := deprecated.LegacyScannerOpts{Unmarshal: func(b []byte, v interface{}) error {
		return fmt.Errorf("oh an unmarshaling error")
	}}
	scanner = deprecated.NewLegacyScanner(buf, scopts)
	scanner.Scan()
	err = scanner.Unmarshal(pb)
	expect.HasSubstr(t, err, "oh an unmarshaling error")
}

func TestPackedMarshalErrors(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	wropts := deprecated.LegacyPackedWriterOpts{}
	writer := deprecated.NewLegacyPackedWriter(buf, wropts)
	_, err := writer.Marshal(&TestPB{"error"})
	expect.HasSubstr(t, err, "Marshal function not configured")

	wropts.Marshal = func(scratch []byte, v interface{}) ([]byte, error) {
		return nil, fmt.Errorf("oh a marshaling error")
	}
	writer = deprecated.NewLegacyPackedWriter(buf, wropts)
	_, err = writer.Marshal(&TestPB{"error"})
	expect.HasSubstr(t, err, "oh a marshaling error")

	wropts.Marshal = recordioMarshal
	writer = deprecated.NewLegacyPackedWriter(buf, wropts)
	_, err = writer.Marshal(&TestPB{"error"})
	if err != nil {
		t.Fatal(err)
	}
	scopts := deprecated.LegacyPackedScannerOpts{}
	scanner := deprecated.NewLegacyPackedScanner(buf, scopts)
	scanner.Scan()
	pb := &TestPB{}
	err = scanner.Unmarshal(pb)
	expect.HasSubstr(t, err, "Unmarshal function not configured")
	scopts.Unmarshal = func(b []byte, v interface{}) error {
		return fmt.Errorf("oh an unmarshaling error")
	}
	scanner = deprecated.NewLegacyPackedScanner(buf, scopts)
	scanner.Scan()
	err = scanner.Unmarshal(pb)
	expect.HasSubstr(t, err, "oh an unmarshaling error")
}
