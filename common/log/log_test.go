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
		DebugLevel: coreLogger.Debugw,
		InfoLevel:  coreLogger.Infow,
		WarnLevel:  coreLogger.Warnw,
		ErrorLevel: coreLogger.Errorw,
	}
	now = func() time.Time {
		return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	}
}

func ExampleDebug() {
	setup()
	Debug(ctx, "Hello, world!")
	Debug(
		ctx,
		"Hello, world!",
		"foo", "bar",
		"abc", 123,
		"time", time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC),
	)
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:36","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"debug","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:37","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"info","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:51","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"info","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:52","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"warn","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:66","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"warn","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:67","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"error","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:81","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"error","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:82","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
}

func ExampleDebugf() {
	setup()
	Debugf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:96","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfof() {
	setup()
	Infof(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:103","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnf() {
	setup()
	Warnf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:110","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorf() {
	setup()
	Errorf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:117","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugv() {
	setup()
	Debugv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:124","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfov() {
	setup()
	Infov(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:131","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnv() {
	setup()
	Warnv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:138","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorv() {
	setup()
	Errorv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:145","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugNoCtx() {
	setup()
	DebugNoCtx("Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:152","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleInfoNoCtx() {
	setup()
	InfoNoCtx("Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:159","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleWarnNoCtx() {
	setup()
	WarnNoCtx("Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:166","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleErrorNoCtx() {
	setup()
	ErrorNoCtx("Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:173","ts":"2000-01-01T00:00:00.000000000Z"}
}

func Example_danglingKey() {
	setup()
	Info(context.Background(), "Hello, world!", "myDanglingKey")
	// Output:
	// {"level":"error","msg":"Ignored key without a value.","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:180","ts":"2000-01-01T00:00:00.000000000Z","ignored":"myDanglingKey"}
	// {"level":"info","msg":"Hello, world!","caller":"go/src/github.com/grailbio/base/common/log/log_test.go:180","ts":"2000-01-01T00:00:00.000000000Z"}
}
