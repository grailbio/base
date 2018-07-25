// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package log

import (
	"flag"
	"fmt"
	"io"
	golog "log"
	"runtime/debug"
	"sync/atomic"
)

var golevel = Info

var called int32 = 0

// AddFlags adds a standard log level flags to the flag.CommandLine
// flag set.
func AddFlags() {
	if atomic.AddInt32(&called, 1) != 1 {
		Error.Printf("log.AddFlags: called twice!")
		debug.PrintStack()
		return
	}
	flag.Var(new(logFlag), "log", "set log level (off, error, info, debug)")
}

// Logger is an alternative spelling of "log".Logger.
type Logger = golog.Logger

const (
	Ldate         = golog.Ldate         // the date in the local time zone: 2009/01/23
	Ltime         = golog.Ltime         // the time in the local time zone: 01:23:23
	Lmicroseconds = golog.Lmicroseconds // microsecond resolution: 01:23:23.123123.  assumes Ltime.
	Llongfile     = golog.Llongfile     // full file name and line number: /a/b/c/d.go:23
	Lshortfile    = golog.Lshortfile    // final file name element and line number: d.go:23. overrides Llongfile
	LUTC          = golog.LUTC          // if Ldate or Ltime is set, use UTC rather than the local time zone
	LstdFlags     = Ldate | Ltime       // initial values for the standard logger
)

// SetFlags sets the output flags for the Go standard logger.
func SetFlags(flag int) {
	golog.SetFlags(flag)
}

// SetOutput sets the output destination for the Go standard logger.
func SetOutput(w io.Writer) {
	golog.SetOutput(w)
}

// SetPrefix sets the output prefix for the Go standard logger.
func SetPrefix(prefix string) {
	golog.SetPrefix(prefix)
}

// SetLevel sets the log level for the Go standard logger.
// It should be called once at the beginning of a program's main.
func SetLevel(level Level) {
	golevel = level
}

type logFlag string

func (f logFlag) String() string {
	return string(f)
}

func (f *logFlag) Set(level string) error {
	var l Level
	switch level {
	case "off":
		l = Off
	case "error":
		l = Error
	case "info":
		l = Info
	case "debug":
		l = Debug
	default:
		return fmt.Errorf("invalid log level %q", level)
	}
	golevel = l
	return nil
}

// Get implements flag.Getter.
func (logFlag) Get() interface{} {
	return golevel
}

type gologOutputter struct{}

func (gologOutputter) Level() Level { return golevel }

func (gologOutputter) Output(calldepth int, level Level, s string) error {
	if golevel < level {
		return nil
	}
	return golog.Output(calldepth+1, s)
}
