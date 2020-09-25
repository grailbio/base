// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package cloudwatch

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
	"github.com/grailbio/base/eventlog/internal/marshal"
)

const testGroup = "testGroup"
const testStream = "testStream"
const typ = "testEventType"
const k = "testFieldKey"

type logsAPIFake struct {
	cloudwatchlogsiface.CloudWatchLogsAPI

	groupInput   *cloudwatchlogs.CreateLogGroupInput
	streamInput  *cloudwatchlogs.CreateLogStreamInput
	eventsInputs []*cloudwatchlogs.PutLogEventsInput

	sequenceMu sync.Mutex
	sequence   int
}

func (f *logsAPIFake) CreateLogGroupWithContext(ctx context.Context,
	input *cloudwatchlogs.CreateLogGroupInput,
	opts ...request.Option) (*cloudwatchlogs.CreateLogGroupOutput, error) {

	f.groupInput = input
	return nil, nil
}

func (f *logsAPIFake) CreateLogStreamWithContext(ctx context.Context,
	input *cloudwatchlogs.CreateLogStreamInput,
	opts ...request.Option) (*cloudwatchlogs.CreateLogStreamOutput, error) {

	f.streamInput = input
	return nil, nil
}

func (f *logsAPIFake) PutLogEventsWithContext(ctx context.Context,
	input *cloudwatchlogs.PutLogEventsInput,
	opts ...request.Option) (*cloudwatchlogs.PutLogEventsOutput, error) {

	var ts *int64
	for _, event := range input.LogEvents {
		if ts != nil && *event.Timestamp < *ts {
			return nil, &cloudwatchlogs.InvalidParameterException{}
		}
		ts = event.Timestamp
	}

	nextSequenceToken, err := func() (*string, error) {
		f.sequenceMu.Lock()
		defer f.sequenceMu.Unlock()
		if f.sequence != 0 {
			sequenceToken := fmt.Sprintf("%d", f.sequence)
			if input.SequenceToken == nil || sequenceToken != *input.SequenceToken {
				return nil, &cloudwatchlogs.InvalidSequenceTokenException{
					ExpectedSequenceToken: &sequenceToken,
				}
			}
		}
		f.sequence++
		nextSequenceToken := fmt.Sprintf("%d", f.sequence)
		return &nextSequenceToken, nil
	}()
	if err != nil {
		return nil, err
	}

	f.eventsInputs = append(f.eventsInputs, input)
	return &cloudwatchlogs.PutLogEventsOutput{
		NextSequenceToken: nextSequenceToken,
	}, nil
}

func (f *logsAPIFake) logEvents() []*cloudwatchlogs.InputLogEvent {
	var events []*cloudwatchlogs.InputLogEvent
	for _, input := range f.eventsInputs {
		events = append(events, input.LogEvents...)
	}
	return events
}

func (f *logsAPIFake) incrNextSequence() {
	f.sequenceMu.Lock()
	defer f.sequenceMu.Unlock()
	f.sequence++
}

// TestEvent verifies that logged events are sent to CloudWatch correctly.
func TestEvent(t *testing.T) {
	const N = 1000

	if eventBufferSize < N {
		panic("keep N <= eventBufferSize to make sure no events are dropped")
	}

	// Log events.
	cw := &logsAPIFake{}
	e := NewCloudWatchEventer(cw, testGroup, testStream)
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("k%d", i)
		e.Event(typ, k, i)
	}
	e.Close()

	// Make sure events get to CloudWatch in order.
	events := cw.logEvents()
	if got, want := len(events), N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for i, event := range events {
		k := fmt.Sprintf("k%d", i)
		m, err := marshal.Marshal(typ, []interface{}{k, i})
		if err != nil {
			t.Fatalf("error marshaling event: %v", err)
		}
		if got, want := *event.Message, m; got != want {
			t.Errorf("got %v, want %v", got, want)
			continue
		}
	}
}

// TestBufferFull verifies that exceeding the event buffer leads to, at worst,
// dropped events. Events that are not dropped should still be logged in order.
func TestBufferFull(t *testing.T) {
	const N = 100 * 1000

	// Log many events, overwhelming buffer.
	cw := &logsAPIFake{}
	e := NewCloudWatchEventer(cw, testGroup, testStream)
	for i := 0; i < N; i++ {
		e.Event(typ, k, i)
	}
	e.Close()

	events := cw.logEvents()
	if N < len(events) {
		t.Fatalf("more events sent to CloudWatch than were logged: %d < %d", N, len(events))
	}
	assertOrdered(t, events)
}

// TestInvalidSequenceToken verifies that we recover if our sequence token gets
// out of sync. This should not happen, as we should be the only thing writing
// to a given log stream, but we try to recover anyway.
func TestInvalidSequenceToken(t *testing.T) {
	cw := &logsAPIFake{}
	e := NewCloudWatchEventer(cw, testGroup, testStream)

	e.Event(typ, k, 0)
	e.sync()
	cw.incrNextSequence()
	e.Event(typ, k, 1)
	e.sync()
	e.Event(typ, k, 2)
	e.sync()
	e.Close()

	events := cw.logEvents()
	if 3 < len(events) {
		t.Fatalf("more events sent to CloudWatch than were logged: 3 < %d", len(events))
	}
	if len(events) < 2 {
		t.Errorf("did not successfully re-sync sequence token")
	}
	assertOrdered(t, events)
}

// assertOrdered asserts that the values of field k are increasing for events.
// This is how we construct events sent to the CloudWatchEventer, so we use this
// verify that the events sent to the CloudWatch Logs API are ordered correctly.
func assertOrdered(t *testing.T, events []*cloudwatchlogs.InputLogEvent) {
	t.Helper()
	last := -1
	for _, event := range events {
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(*event.Message), &m); err != nil {
			t.Fatalf("could not unmarshal event message: %v", err)
		}
		v, ok := m[k]
		if !ok {
			t.Errorf("event message does not contain test key %q: %s", k, *event.Message)
			continue
		}
		// All numeric values are unmarshaled as float64, so we need to convert
		// back to int.
		vi := int(v.(float64))
		if vi <= last {
			t.Errorf("event out of order; expected %d < %d", last, vi)
			continue
		}
	}
}
