// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package cloudwatch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
	"github.com/grailbio/base/config"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/eventlog/internal/marshal"
	"github.com/grailbio/base/log"
)

// maxBatchSize is the maximum batch size of the events that we send to CloudWatch Logs, "calculated
// as the sum of all event messages in UTF-8, plus 26 bytes for each log event". See:
// https://docs.aws.amazon.com/sdk-for-go/api/service/cloudwatchlogs/#CloudWatchLogs.PutLogEvents
const maxBatchSize = 1048576

// maxSingleMessageSize is the maximum size, in bytes, of a single message
// string of CloudWatch Log event.
const maxSingleMessageSize = maxBatchSize - 26

// syncInterval is the maximum duration that we will wait before sending any
// buffered messages to CloudWatch.
const syncInterval = 1 * time.Second

// eventBufferSize is size of the channel buffer used to process events. When
// this buffer is full, new events are dropped.
const eventBufferSize = 32768

func init() {
	config.Register("eventer/cloudwatch", func(constr *config.Constructor) {
		var sess *session.Session
		constr.InstanceVar(&sess, "aws", "aws", "AWS configuration for all CloudWatch calls")
		var group string
		constr.StringVar(&group, "group", "eventlog", "the CloudWatch log group of the stream to which events will be sent")
		var stream string
		constr.StringVar(&stream, "stream", "", "the CloudWatch log stream to which events will be sent")
		constr.Doc = "eventer/cloudwatch configures an eventer that sends events to a CloudWatch log stream"
		constr.New = func() (interface{}, error) {
			cw := cloudwatchlogs.New(sess)
			if stream == "" {
				// All the information of RFC 3339 but without colons, as stream
				// names cannot have colons.
				const layout = "20060102T150405.000-0700"
				// The default stream name incorporates the current executable
				// name and time for some measure of uniqueness and usefulness.
				stream = strings.Join([]string{readExec(), time.Now().Format(layout)}, "~")
			}
			return NewCloudWatchEventer(cw, group, stream), nil
		}
	})
}

// CloudWatchEventer logs events to CloudWatch Logs.
type CloudWatchEventer struct {
	client cloudwatchlogsiface.CloudWatchLogsAPI
	group  string
	stream string

	cancel func()

	// eventc is used to send events that are batched and sent to CloudWatch
	// Logs.
	eventc chan event

	// syncc is used to force syncing of events to CloudWatch Logs.
	syncc chan struct{}
	// syncDonec is used to block for syncing.
	syncDonec chan struct{}

	// donec is used to signal that the event processing loop is done.
	donec chan struct{}

	initOnce sync.Once
	initErr  error

	sequenceToken *string

	// loggedFullBuffer prevents consecutive printing of "buffer full" log
	// messages inside Event. We get a full buffer when we are overloaded with
	// many messages. Logging each dropped message is very noisy, so we suppress
	// consecutive logging.
	loggedFullBuffer int32
}

type event struct {
	timestamp  time.Time
	typ        string
	fieldPairs []interface{}
}

// NewCloudWatchEventer returns a *CloudWatchLogger. It does create the group or
// stream until the first event is logged.
func NewCloudWatchEventer(client cloudwatchlogsiface.CloudWatchLogsAPI, group, stream string) *CloudWatchEventer {
	eventer := &CloudWatchEventer{
		client:    client,
		group:     group,
		stream:    stream,
		eventc:    make(chan event, eventBufferSize),
		syncc:     make(chan struct{}),
		syncDonec: make(chan struct{}),
		donec:     make(chan struct{}),
	}
	var ctx context.Context
	ctx, eventer.cancel = context.WithCancel(context.Background())
	go eventer.loop(ctx)
	return eventer
}

// Event implements Eventer.
func (c *CloudWatchEventer) Event(typ string, fieldPairs ...interface{}) {
	select {
	case c.eventc <- event{timestamp: time.Now(), typ: typ, fieldPairs: fieldPairs}:
		atomic.StoreInt32(&c.loggedFullBuffer, 0)
	default:
		if atomic.LoadInt32(&c.loggedFullBuffer) == 0 {
			log.Error.Printf("CloudWatchEventer: dropping log events: buffer full")
			atomic.StoreInt32(&c.loggedFullBuffer, 1)
		}
	}
}

// Init initializes the group and stream used by c. It will only attempt
// initialization once, subsequently returning the result of that attempt.
func (c *CloudWatchEventer) Init(ctx context.Context) error {
	c.initOnce.Do(func() {
		defer func() {
			if c.initErr != nil {
				log.Error.Printf("CloudWatchEventer: failed to initialize event log: %v", c.initErr)
			}
		}()
		var err error
		_, err = c.client.CreateLogGroupWithContext(ctx, &cloudwatchlogs.CreateLogGroupInput{
			LogGroupName: aws.String(c.group),
		})
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if !ok || aerr.Code() != cloudwatchlogs.ErrCodeResourceAlreadyExistsException {
				c.initErr = errors.E(fmt.Sprintf("could not create CloudWatch log group %s", c.group), err)
				return
			}
		}
		_, err = c.client.CreateLogStreamWithContext(ctx, &cloudwatchlogs.CreateLogStreamInput{
			LogGroupName:  aws.String(c.group),
			LogStreamName: aws.String(c.stream),
		})
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if ok && aerr.Code() != cloudwatchlogs.ErrCodeResourceAlreadyExistsException {
				c.initErr = errors.E(fmt.Sprintf("could not create CloudWatch log stream %s", c.stream), err)
				return
			}
		}
	})
	return c.initErr
}

// sync syncs all buffered events to CloudWatch. This is mostly useful for
// testing.
func (c *CloudWatchEventer) sync() {
	c.syncc <- struct{}{}
	<-c.syncDonec
}

func (c *CloudWatchEventer) Close() error {
	c.cancel()
	<-c.donec
	return nil
}

func (c *CloudWatchEventer) loop(ctx context.Context) {
	var (
		syncTimer      = time.NewTimer(syncInterval)
		inputLogEvents []*cloudwatchlogs.InputLogEvent
		batchSize      int
	)
	sync := func(drainTimer bool) {
		defer func() {
			inputLogEvents = nil
			batchSize = 0
			if !syncTimer.Stop() && drainTimer {
				<-syncTimer.C
			}
			syncTimer.Reset(syncInterval)
		}()
		if len(inputLogEvents) == 0 {
			return
		}
		if err := c.Init(ctx); err != nil {
			return
		}
		response, err := c.client.PutLogEventsWithContext(ctx, &cloudwatchlogs.PutLogEventsInput{
			LogEvents:     inputLogEvents,
			LogGroupName:  aws.String(c.group),
			LogStreamName: aws.String(c.stream),
			SequenceToken: c.sequenceToken,
		})
		if err != nil {
			log.Error.Printf("CloudWatchEventer: PutLogEvents error: %v", err)
			if aerr, ok := err.(*cloudwatchlogs.InvalidSequenceTokenException); ok {
				c.sequenceToken = aerr.ExpectedSequenceToken
			}
			return
		}
		c.sequenceToken = response.NextSequenceToken
	}
	process := func(e event) {
		s, err := marshal.Marshal(e.typ, e.fieldPairs)
		if err != nil {
			log.Error.Printf("CloudWatchEventer: dropping log event: %v", err)
			return
		}
		if len(s) > maxSingleMessageSize {
			log.Error.Printf("CloudWatchEventer: dropping log event: message too large")
			return
		}
		newBatchSize := batchSize + len(s) + 26
		if newBatchSize > maxBatchSize {
			sync(true)
		}
		inputLogEvents = append(inputLogEvents, &cloudwatchlogs.InputLogEvent{
			Message:   aws.String(s),
			Timestamp: aws.Int64(e.timestamp.UnixNano() / 1000000),
		})
	}
	drainEvents := func() {
	drainLoop:
		for {
			select {
			case e := <-c.eventc:
				process(e)
			default:
				break drainLoop
			}
		}
	}
	for {
		select {
		case <-c.syncc:
			drainEvents()
			sync(false)
			c.syncDonec <- struct{}{}
		case <-syncTimer.C:
			sync(false)
		case e := <-c.eventc:
			process(e)
		case <-ctx.Done():
			close(c.eventc)
			for e := range c.eventc {
				process(e)
			}
			sync(true)
			close(c.donec)
			return
		}
	}
}

// readExec returns a sanitized version of the executable name, if it can be
// determined. If not, returns "unknown".
func readExec() string {
	const unknown = "unknown"
	execPath, err := os.Executable()
	if err != nil {
		return unknown
	}
	rawExec := filepath.Base(execPath)
	var sanitized strings.Builder
	for _, r := range rawExec {
		if (r == '-' || 'a' <= r && r <= 'z') || ('0' <= r && r <= '9') {
			sanitized.WriteRune(r)
		}
	}
	if sanitized.Len() == 0 {
		return unknown
	}
	return sanitized.String()
}
