package log

import (
	"context"
	"time"

	"github.com/google/uuid"
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
	logger = NewLogger()
	logger.now = func() time.Time {
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
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:28","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:29","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:43","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:44","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:58","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:59","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:73","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:74","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
}

func ExampleDebugf() {
	setup()
	Debugf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:88","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfof() {
	setup()
	Infof(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:95","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnf() {
	setup()
	Warnf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:102","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorf() {
	setup()
	Errorf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:109","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugv() {
	setup()
	Debugv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:116","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfov() {
	setup()
	Infov(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:123","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnv() {
	setup()
	Warnv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:130","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorv() {
	setup()
	Errorv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:137","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugNoCtx() {
	setup()
	DebugNoCtx("Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:144","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleInfoNoCtx() {
	setup()
	InfoNoCtx("Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:151","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleWarnNoCtx() {
	setup()
	WarnNoCtx("Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:158","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleErrorNoCtx() {
	setup()
	ErrorNoCtx("Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:165","ts":"2000-01-01T00:00:00.000000000Z"}
}

func Example_danglingKey() {
	setup()
	Info(context.Background(), "Hello, world!", "myDanglingKey")
	// Output:
	// {"level":"error","msg":"Ignored key without a value.","caller":"log_test.go:172","ts":"2000-01-01T00:00:00.000000000Z","ignored":"myDanglingKey"}
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:172","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleDebug_logger() {
	setup()
	logger.Debug(ctx, "Hello, world!")
	logger.Debug(
		ctx,
		"Hello, world!",
		"foo", "bar",
		"abc", 123,
		"time", time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC),
	)
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:180","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:181","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
}

func ExampleInfo_logger() {
	setup()
	logger.Info(ctx, "Hello, world!")
	logger.Info(
		ctx,
		"Hello, world!",
		"foo", "bar",
		"abc", 123,
		"time", time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC),
	)
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:195","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:196","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
}

func ExampleWarn_logger() {
	setup()
	logger.Warn(ctx, "Hello, world!")
	logger.Warn(
		ctx,
		"Hello, world!",
		"foo", "bar",
		"abc", 123,
		"time", time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC),
	)
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:210","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:211","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
}

func ExampleError_logger() {
	setup()
	logger.Error(ctx, "Hello, world!")
	logger.Error(
		ctx,
		"Hello, world!",
		"foo", "bar",
		"abc", 123,
		"time", time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC),
	)
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:225","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:226","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
}

func ExampleDebugf_logger() {
	setup()
	logger.Debugf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:240","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfof_logger() {
	setup()
	logger.Infof(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:247","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnf_logger() {
	setup()
	logger.Warnf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:254","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorf_logger() {
	setup()
	logger.Errorf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:261","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugv_logger() {
	setup()
	logger.Debugv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:268","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfov_logger() {
	setup()
	logger.Infov(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:275","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnv_logger() {
	setup()
	logger.Warnv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:282","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorv_logger() {
	setup()
	logger.Errorv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:289","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugNoCtx_logger() {
	setup()
	logger.DebugNoCtx("Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:296","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleInfoNoCtx_logger() {
	setup()
	logger.InfoNoCtx("Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:303","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleWarnNoCtx_logger() {
	setup()
	logger.WarnNoCtx("Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:310","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleErrorNoCtx_logger() {
	setup()
	logger.ErrorNoCtx("Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:317","ts":"2000-01-01T00:00:00.000000000Z"}
}
