// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package log provides simple level logging. Log output is
// implemented by an outputter, which by default outputs to Go's
// logging package. Alternative implementations (e.g., vlog, glog)
// can provide their own outputter implementation so that logging
// output is unified.
//
// The package can be used as a replacement for Go's standard logging
// package; the behavior of its toplevel functions are identical with
// the default configuration.
//
// If the application wishes to configure logging levels by standard
// flags, it should call log.AddFlags before flag.Parse.
// log.AddFlags.
//
// Note that this package is intended to bridge Go's standard log
// package with Vanadium's (vlog). As we transition away from vlog,
// we can simplify this package by removing the Outputter interface.
package log

import (
	"fmt"
	"os"
)

// An Outputter provides a destination for leveled log output.
type Outputter interface {
	// Level returns the level at which the outputter is accepting
	// messages.
	Level() Level

	// Output writes the provided message to the outputter at the
	// provided calldepth and level. The message is dropped by
	// the outputter if it is not logging at the desired level.
	Output(calldepth int, level Level, s string) error
}

var out Outputter = gologOutputter{}

// SetOutputter provides a new outputter for use in the log package.
// SetOutputter should not be called concurrently with any log
// output, and is thus suitable to be called only upon program
// initialization. SetOutputter returns the old outputter.
func SetOutputter(newOut Outputter) Outputter {
	old := out
	out = newOut
	return old
}

// GetOutputter returns the current outputter used by the log package.
func GetOutputter() Outputter {
	return out
}

// At returns whether the logger is currently logging at the provided level.
func At(level Level) bool {
	return level <= out.Level()
}

// Output outputs a log message to the current outputter at the provided
// level and call depth.
func Output(calldepth int, level Level, s string) error {
	return out.Output(calldepth+1, level, s)
}

// A Level is a log verbosity level. Increasing levels decrease in
// priority and (usually) increase in verbosity: if the outputter is
// logging at level L, then all messages with level M <= L are
// outputted.
type Level int

const (
	// Off never outputs messages.
	Off = Level(-3)
	// Error outputs error messages.
	Error = Level(-2)
	// Info outputs informational messages. This is the standard
	// logging level.
	Info = Level(0)
	// Debug outputs messages intended for debugging and development,
	// not for regular users.
	Debug = Level(1)
)

// String returns the string representation of the level l.
func (l Level) String() string {
	switch l {
	case Off:
		return "off"
	case Error:
		return "error"
	case Info:
		return "info"
	case Debug:
		return "debug"
	default:
		if l < 0 {
			panic("invalid log level")
		}
		return fmt.Sprintf("debug%d", l)
	}
}

// Print formats a message in the manner of fmt.Sprint and outputs it
// at level l to the current outputter.
func (l Level) Print(v ...interface{}) {
	if At(l) {
		out.Output(2, l, fmt.Sprint(v...))
	}
}

// Printkln formats a message in the manner of fmt.Sprintln and outputs
// it at level l to the current outputter.
func (l Level) Println(v ...interface{}) {
	if At(l) {
		out.Output(2, l, fmt.Sprintln(v...))
	}
}

// Printf formats a message in the manner of fmt.Sprintf and outputs
// it at level l to the current outputter.
func (l Level) Printf(format string, v ...interface{}) {
	if At(l) {
		out.Output(2, l, fmt.Sprintf(format, v...))
	}
}

// Print formats a message in the manner of fmt.Sprint
// and outputs it at the Info level to the current outputter.
func Print(v ...interface{}) {
	if At(Info) {
		out.Output(2, Info, fmt.Sprint(v...))
	}
}

// Printf formats a message in the manner of fmt.Sprintf
// and outputs it at the Info level to the current outputter.
func Printf(format string, v ...interface{}) {
	if At(Info) {
		out.Output(2, Info, fmt.Sprintf(format, v...))
	}
}

// Fatal formats a message in the manner of fmt.Sprint, outputs it at
// the error level to the current outputter and then calls
// os.Exit(1).
func Fatal(v ...interface{}) {
	out.Output(2, Error, fmt.Sprint(v...))
	os.Exit(1)
}

// Fatalf formats a message in the manner of fmt.Sprintf, outputs it at
// the error level to the current outputter and then calls
// os.Exit(1).
func Fatalf(format string, v ...interface{}) {
	out.Output(2, Error, fmt.Sprintf(format, v...))
	os.Exit(1)
}

// Panic formats a message in the manner of fmt.Sprint, outputs it
// at the error level to the current outputter and then panics.
func Panic(v ...interface{}) {
	s := fmt.Sprint(v...)
	out.Output(2, Error, s)
	panic(s)
}

// Panicf formats a message in the manner of fmt.Sprintf, outputs it
// at the error level to the current outputter and then panics.
func Panicf(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	out.Output(2, Error, s)
	panic(s)
}

// Outputf is formats a message using fmt.Sprintf and outputs it
// to the provided logger at the provided level.
func Outputf(out Outputter, level Level, format string, v ...interface{}) {
	out.Output(2, level, fmt.Sprintf(format, v...))
}
