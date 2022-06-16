// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package eventlog provides logging of semi-structured events, particularly in
// service of downstream analysis, e.g. when machines are started, when a user
// issues a command, when failures happen.
//
// For example, you can log events to CloudWatch Logs:
//
//  sess := session.NewSession()
//  cw := cloudwatchlogs.New(sess)
//  e := cloudwatch.NewEventer(cw, "myLogGroup", "myLogStream")
//  e.Event("rpcRetry", "org", "GRAIL", "retry", 0, "maxRetry", 10)
//  e.Event("machineStopped", "addr", "192.168.1.1", "duration", 3600.0, "startTime": 1584140534)
//
// These events can now be analyzed and monitored using CloudWatch Logs tooling.
package eventlog

import (
	"bytes"

	"github.com/grailbio/base/config"
	"github.com/grailbio/base/log"
)

func init() {
	config.Register("eventer/nop", func(constr *config.Constructor) {
		constr.Doc = "eventer/nop configures a no-op event logger"
		constr.New = func() (interface{}, error) {
			return Nop{}, nil
		}
	})
	config.Register("eventer/log-info", func(constr *config.Constructor) {
		constr.Doc = "eventer/log-info configures an eventer that writes events at log level info"
		constr.New = func() (interface{}, error) {
			return Log(log.Info), nil
		}
	})
	// We do the most conservative thing by default, making event logging a
	// no-op.
	config.Default("eventer", "eventer/nop")
}

// Eventer is called to log events.
type Eventer interface {
	// Event logs an event of typ with (key string, value interface{}) fields given in fieldPairs
	// as k0, v0, k1, v1, ...kn, vn. For example:
	//
	//  s.Event("machineStart", "addr", "192.168.1.2", "time", time.Now().Unix())
	//
	// The value will be serialized as JSON.
	//
	// The key "eventType" is reserved. Field keys must be unique. Any violation will result
	// in the event being dropped and logged.
	//
	// Implementations must be safe for concurrent use.
	Event(typ string, fieldPairs ...interface{})
}

// Nop is a no-op Eventer.
type Nop struct{}

var _ Eventer = Nop{}

func (Nop) String() string {
	return "disabled"
}

// Event implements Eventer.
func (Nop) Event(_ string, _ ...interface{}) {}

// Log is an Eventer that writes events to the logger. It's intended for debugging/development.
type Log log.Level

var _ Eventer = Log(log.Debug)

// Event implements Eventer.
func (l Log) Event(typ string, fieldPairs ...interface{}) {
	f := bytes.NewBufferString("eventlog: %s {")
	for i := range fieldPairs {
		f.WriteString("%v")
		if i%2 == 0 {
			f.WriteString(": ")
		} else if i < len(fieldPairs)-1 {
			f.WriteString(", ")
		}
	}
	f.WriteByte('}')
	log.Level(l).Printf(f.String(), append([]interface{}{typ}, fieldPairs...)...)
}
