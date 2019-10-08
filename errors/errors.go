// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package errors implements an error type that defines standard
// interpretable error codes for common error conditions. Errors also
// contain interpretable severities, so that error-producing
// operations can be retried in consistent ways. Errors returned by
// this package can also be chained: thus attributing one error to
// another. It is inspired by the error packages of both the Upspin
// and Reflow projects, but generalizes and simplifies these.
//
// Errors are safely serialized with Gob, and can thus retain
// semantics across process boundaries.
//
// TODO(marius): standardize translating AWS errors into *errors.Error.
package errors

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/grailbio/base/log"
)

func init() {
	gob.Register(new(Error))
}

// Separator defines the separation string inserted between
// chained errors in error messages.
var Separator = ":\n\t"

// Kind defines the type of error. Kinds are semantically
// meaningful, and may be interpreted by the receiver of an error
// (e.g., to determine whether an operation should be retried).
type Kind int

const (
	// Other indicates an unknown error.
	Other Kind = iota
	// Canceled indicates a context cancellation.
	Canceled
	// Timeout indicates an operation time out.
	Timeout
	// NotExist indicates a nonexistent resources.
	NotExist
	// NotAllowed indicates a permission failure.
	NotAllowed
	// NotSupported indicates an unsupported operation.
	NotSupported
	// Exists indicates that a resource already exists.
	Exists
	// Integrity indicates an integrity failure.
	Integrity
	// Unavailable indicates that a resource was unavailable.
	Unavailable
	// Invalid indicates that the caller supplied invalid parameters.
	Invalid
	// Net indicates a network error.
	Net
	// TooManyTries indicates a retry budget was exhausted.
	TooManyTries
	// Precondition indicates that a precondition was not met.
	Precondition
	// OOM indicates that an OOM condition was encountered.
	OOM
	// Remote indicates an error returned by an RPC, as distinct from errors in
	// the machinery to execute the RPC, e.g. network issues, machine health,
	// etc.
	Remote

	maxKind
)

var kinds = map[Kind]string{
	Other:        "unknown error",
	Canceled:     "operation was canceled",
	Timeout:      "operation timed out",
	NotExist:     "resource does not exist",
	NotAllowed:   "access denied",
	NotSupported: "operation not supported",
	Exists:       "resource already exists",
	Integrity:    "integrity error",
	Unavailable:  "resource unavailable",
	Invalid:      "invalid argument",
	Net:          "network error",
	TooManyTries: "too many tries",
	Precondition: "precondition failed",
	OOM:          "out of memory",
	Remote:       "remote error",
}

// String returns a human-readable explanation of the error kind k.
func (k Kind) String() string {
	return kinds[k]
}

// Severity defines an Error's severity. An Error's severity determines
// whether an error-producing operation may be retried or not.
type Severity int

const (
	// Retriable indicates that the failing operation can be safely retried,
	// regardless of application context.
	Retriable Severity = -2
	// Temporary indicates that the underlying error condition is likely
	// temporary, and can be possibly be retried. However, such errors
	// should be retried in an application specific context.
	Temporary Severity = -1
	// Unknown indicates the error's severity is unknown. This is the default
	// severity level.
	Unknown Severity = 0
	// Fatal indicates that the underlying error condition is unrecoverable;
	// retrying is unlikely to help.
	Fatal Severity = 1
)

var severities = map[Severity]string{
	Retriable: "retriable",
	Temporary: "temporary",
	Unknown:   "unknown",
	Fatal:     "fatal",
}

// String returns a human-readable explanation of the error severity s.
func (s Severity) String() string {
	return severities[s]
}

// Error is the standard error type, carrying a kind (error code),
// message (error message), and potentially an underlying error.
// Errors should be constructed by errors.E, which interprets
// arguments according to a set of rules.
//
// Errors may be serialized and deserialized with gob. When this is
// done, underlying errors do not survive in full fidelity: they are
// converted to their error strings and returned as opaque errors.
type Error struct {
	// Kind is the error's type.
	Kind Kind
	// Severity is an optional severity.
	Severity Severity
	// Message is an optional error message associated with this error.
	Message string
	// Err is the error that caused this error, if any.
	// Errors can form chains through Err: the full chain is printed
	// by Error().
	Err error
}

// E constructs a new errors from the provided arguments. It is meant
// as a convenient way to construct, annotate, and wrap errors.
//
// Arguments are interpreted according to their types:
//
//	- Kind: sets the Error's kind
//	- Severity: set the Error's severity
//	- string: sets the Error's message; multiple strings are
//	  separated by a single space
//	- *Error: copies the error and sets the error's cause
//	- error: sets the Error's cause
//
// If an unrecognized argument type is encountered, an error with
// kind Invalid is returned.
//
// If a kind is not provided, but an underlying error is, E attempts to
// interpret the underlying error according to a set of conventions,
// in order:
//
//	- If the os.IsNotExist(error) returns true, its kind is set to NotExist.
// 	- If the error is context.Canceled, its kind is set to Canceled.
//	- If the error implements interface { Timeout() bool } and
//	Timeout() returns true, then its kind is set to Timeout
//	- If the error implements interface { Temporary() bool } and
//	Temporary() returns true, then its severity is set to at least
//	Temporary.
//
// If the underlying error is another *Error, and a kind is not provided,
// the returned error inherits that error's kind.
func E(args ...interface{}) error {
	if len(args) == 0 {
		panic("no args")
	}
	e := new(Error)
	var msg strings.Builder
	for _, arg := range args {
		switch arg := arg.(type) {
		case Kind:
			e.Kind = arg
		case Severity:
			e.Severity = arg
		case string:
			if msg.Len() > 0 {
				msg.WriteString(" ")
			}
			msg.WriteString(arg)
		case *Error:
			copy := *arg
			if len(args) == 1 {
				// In this case, we're not adding anything new;
				// just return the copy.
				return &copy
			}
			e.Err = &copy
		case error:
			e.Err = arg
		default:
			_, file, line, _ := runtime.Caller(1)
			log.Error.Printf("errors.E: bad call (type %T) from %s:%d: %v", arg, file, line, arg)
			return &Error{
				Kind:    Invalid,
				Message: fmt.Sprintf("unknown type %T, value %v in error call", arg, arg),
			}
		}
	}
	e.Message = msg.String()
	if e.Err == nil {
		return e
	}
	switch prev := e.Err.(type) {
	case *Error:
		// TODO(marius): consider collapsing e.Err altogether if
		// all we're doing is adding a Kind and/or Severity.
		if prev.Kind == e.Kind || e.Kind == Other {
			e.Kind = prev.Kind
			prev.Kind = Other
		}
		if prev.Severity == e.Severity || e.Severity == Unknown {
			e.Severity = prev.Severity
			prev.Severity = Unknown
		}
	default:
		// Classify common error types.
		if err, ok := e.Err.(interface {
			Temporary() bool
		}); ok && err.Temporary() && e.Severity == Unknown {
			e.Severity = Temporary
		}
		if e.Kind != Other {
			break
		}
		if os.IsNotExist(e.Err) {
			e.Kind = NotExist
		} else if e.Err == context.Canceled {
			e.Kind = Canceled
		} else if err, ok := e.Err.(interface {
			Timeout() bool
		}); ok && err.Timeout() {
			e.Kind = Timeout
		}
	}
	return e
}

// Recover recovers any error into an *Error. If the passed-in Error is already
// an error, it is simply returned; otherwise it is wrapped in an error.
func Recover(err error) *Error {
	if err == nil {
		return nil
	}
	if err, ok := err.(*Error); ok {
		return err
	}
	return E(err).(*Error)
}

// Error returns a human readable string describing this error.
// It uses the separator defined by errors.Separator.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	var b bytes.Buffer
	e.writeError(&b)
	return b.String()
}

func (e *Error) writeError(b *bytes.Buffer) {
	if e.Message != "" {
		pad(b, ": ")
		b.WriteString(e.Message)
	}
	if e.Kind != Other {
		pad(b, ": ")
		b.WriteString(e.Kind.String())
	}
	if e.Severity != Unknown {
		pad(b, " ")
		b.WriteByte('(')
		b.WriteString(e.Severity.String())
		b.WriteByte(')')
	}

	if e.Err == nil {
		return
	}
	if err, ok := e.Err.(*Error); ok {
		pad(b, Separator)
		b.WriteString(err.Error())
	} else {
		pad(b, ": ")
		b.WriteString(e.Err.Error())
	}
}

// Timeout tells whether this error is a timeout error.
func (e *Error) Timeout() bool {
	return e.Kind == Timeout
}

// Temporary tells whether this error is temporary.
func (e *Error) Temporary() bool {
	return e.Severity <= Temporary
}

type gobError struct {
	Kind     Kind
	Severity Severity
	Message  string
	Next     *gobError
	Err      string
}

func (ge *gobError) toError() *Error {
	e := &Error{
		Kind:     ge.Kind,
		Severity: ge.Severity,
		Message:  ge.Message,
	}
	if ge.Next != nil {
		e.Err = ge.Next.toError()
	} else if ge.Err != "" {
		e.Err = errors.New(ge.Err)
	}
	return e
}

func (e *Error) toGobError() *gobError {
	ge := &gobError{
		Kind:     e.Kind,
		Severity: e.Severity,
		Message:  e.Message,
	}
	if e.Err == nil {
		return ge
	}
	switch arg := e.Err.(type) {
	case *Error:
		ge.Next = arg.toGobError()
	default:
		ge.Err = arg.Error()
	}
	return ge
}

// GobEncode encodes the error for gob. Since underlying errors may
// be interfaces unknown to gob, Error's gob encoding replaces these
// with error strings.
func (e *Error) GobEncode() ([]byte, error) {
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(e.toGobError())
	return b.Bytes(), err
}

// GobDecode decodes an error encoded by GobEncode.
func (e *Error) GobDecode(p []byte) error {
	var ge gobError
	if err := gob.NewDecoder(bytes.NewBuffer(p)).Decode(&ge); err != nil {
		return err
	}
	*e = *ge.toError()
	return nil
}

// Is tells whether an error has a specified kind, except for the
// indeterminate kind Other. In the case an error has kind Other, the
// chain is traversed until a non-Other error is encountered.
func Is(kind Kind, err error) bool {
	if err == nil {
		return false
	}
	return is(kind, Recover(err))
}

func is(kind Kind, e *Error) bool {
	if e.Kind != Other {
		return e.Kind == kind
	}
	if e.Err != nil {
		if e2, ok := e.Err.(*Error); ok {
			return is(kind, e2)
		}
	}
	return false
}

// IsTemporary tells whether the provided error is likely temporary.
func IsTemporary(err error) bool {
	return Recover(err).Temporary()
}

// Match tells whether every nonempty field in err1
// matches the corresponding fields in err2. The comparison
// recurses on chained errors. Match is designed to aid in
// testing errors.
func Match(err1, err2 error) bool {
	var (
		e1 = Recover(err1)
		e2 = Recover(err2)
	)
	if e1.Kind != Other && e1.Kind != e2.Kind {
		return false
	}
	if e1.Severity != Unknown && e1.Severity != e2.Severity {
		return false
	}
	if e1.Message != "" && e1.Message != e2.Message {
		return false
	}
	if e1.Err != nil {
		if e2.Err == nil {
			return false
		}
		switch e1.Err.(type) {
		case *Error:
			return Match(e1.Err, e2.Err)
		default:
			return e1.Err.Error() == e2.Err.Error()
		}
	}
	return true
}

// Visit calls the given function for every error object in the chain, including
// itself.  Recursion stops after the function finds an error object of type
// other than *Error.
func Visit(err error, callback func(err error)) {
	callback(err)
	for {
		next, ok := err.(*Error)
		if !ok {
			break
		}
		err = next.Err
		callback(err)
	}
}

// New is synonymous with errors.New, and is provided here so that
// users need only import one errors package.
func New(msg string) error {
	return errors.New(msg)
}

func pad(b *bytes.Buffer, s string) {
	if b.Len() == 0 {
		return
	}
	b.WriteString(s)
}
