package log

import (
	"context"
	"time"

	"github.com/google/uuid"
)

var (
	requestID  uuid.UUID
	ctx        context.Context
	TestConfig = Config{
		OutputPaths: []string{"stdout"},
	}
)

func setup() {
	requestID, _ = uuid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	ctx = WithRequestID(context.Background(), requestID)
	logger = NewLogger(TestConfig)
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
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:29","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:30","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:44","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:45","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:59","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:60","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:74","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:75","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
}

func ExampleDebugf() {
	setup()
	Debugf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:89","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfof() {
	setup()
	Infof(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:96","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnf() {
	setup()
	Warnf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:103","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorf() {
	setup()
	Errorf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:110","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugv() {
	setup()
	Debugv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:117","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfov() {
	setup()
	Infov(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:124","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnv() {
	setup()
	Warnv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:131","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorv() {
	setup()
	Errorv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:138","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugNoCtx() {
	setup()
	DebugNoCtx("Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:145","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleInfoNoCtx() {
	setup()
	InfoNoCtx("Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:152","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleWarnNoCtx() {
	setup()
	WarnNoCtx("Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:159","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleErrorNoCtx() {
	setup()
	ErrorNoCtx("Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:166","ts":"2000-01-01T00:00:00.000000000Z"}
}

func Example_danglingKey() {
	setup()
	Info(context.Background(), "Hello, world!", "myDanglingKey")
	// Output:
	// {"level":"error","msg":"Ignored key without a value.","caller":"log_test.go:173","ts":"2000-01-01T00:00:00.000000000Z","ignored":"myDanglingKey"}
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:173","ts":"2000-01-01T00:00:00.000000000Z"}
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
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:181","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:182","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:196","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:197","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:211","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:212","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:226","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:227","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
}

func ExampleDebugf_logger() {
	setup()
	logger.Debugf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:241","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfof_logger() {
	setup()
	logger.Infof(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:248","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnf_logger() {
	setup()
	logger.Warnf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:255","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorf_logger() {
	setup()
	logger.Errorf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:262","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugv_logger() {
	setup()
	logger.Debugv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:269","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfov_logger() {
	setup()
	logger.Infov(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:276","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnv_logger() {
	setup()
	logger.Warnv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:283","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorv_logger() {
	setup()
	logger.Errorv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:290","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugNoCtx_logger() {
	setup()
	logger.DebugNoCtx("Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:297","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleInfoNoCtx_logger() {
	setup()
	logger.InfoNoCtx("Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:304","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleWarnNoCtx_logger() {
	setup()
	logger.WarnNoCtx("Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:311","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleErrorNoCtx_logger() {
	setup()
	logger.ErrorNoCtx("Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:318","ts":"2000-01-01T00:00:00.000000000Z"}
}
