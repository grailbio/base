package log

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	requestID uuid.UUID
	ctx       context.Context
)

func setup() {
	requestID, _ = uuid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	ctx = WithRequestID(context.Background(), requestID)
	// To support testing, the loggers must be instantiated after the start of the test.
	// Otherwise, outputs do not get properly sent to stdout.
	coreLogger = mustBuildLogger(zap.AddCallerSkip(2))
	levelToLogger = map[zapcore.Level]func(msg string, keysAndValues ...interface{}){
		InfoLevel:  coreLogger.Infow,
		WarnLevel:  coreLogger.Warnw,
		ErrorLevel: coreLogger.Errorw,
	}
	now = func() time.Time {
		return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	}
}

func ExampleInfo() {
	setup()
	Info(ctx, "Hello, world!")
	Info(
		ctx,
		"Hello, world!",
		"foo", "bar",
		"abc", 123,
		"time", time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC),
	)
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:35","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"info","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:36","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
}

func ExampleWarn() {
	setup()
	Warn(ctx, "Hello, world!")
	Warn(
		ctx,
		"Hello, world!",
		"foo", "bar",
		"abc", 123,
		"time", time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC),
	)
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:50","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"warn","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:51","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
}

func ExampleError() {
	setup()
	Error(ctx, "Hello, world!")
	Error(
		ctx,
		"Hello, world!",
		"foo", "bar",
		"abc", 123,
		"time", time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC),
	)
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:65","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"error","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:66","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
}

func ExampleInfof() {
	setup()
	Infof(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:80","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnf() {
	setup()
	Warnf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:87","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorf() {
	setup()
	Errorf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:94","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfov() {
	setup()
	Infov(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:101","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnv() {
	setup()
	Warnv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:108","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorv() {
	setup()
	Errorv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:115","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func Example_danglingKey() {
	setup()
	Info(context.Background(), "Hello, world!", "myDanglingKey")
	// Output:
	// {"level":"error","msg":"Ignored key without a value.","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:122","ts":"2000-01-01T00:00:00.000000000Z","ignored":"myDanglingKey"}
	// {"level":"info","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:122","ts":"2000-01-01T00:00:00.000000000Z"}
}
