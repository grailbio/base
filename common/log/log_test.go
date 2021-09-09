package log

import (
	"context"
	"os"
	"time"

	"github.com/google/uuid"
)

var (
	requestID  uuid.UUID
	ctx        context.Context
	TestConfig = Config{
		OutputPaths: []string{"stdout"},
		Level:       DebugLevel,
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
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:31","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:32","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:46","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:47","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:61","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:62","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:76","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:77","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
}

func ExampleDebugf() {
	setup()
	Debugf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:91","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfof() {
	setup()
	Infof(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:98","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnf() {
	setup()
	Warnf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:105","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorf() {
	setup()
	Errorf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:112","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugv() {
	setup()
	Debugv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:119","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfov() {
	setup()
	Infov(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:126","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnv() {
	setup()
	Warnv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:133","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorv() {
	setup()
	Errorv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:140","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugNoCtx() {
	setup()
	DebugNoCtx("Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:147","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleInfoNoCtx() {
	setup()
	InfoNoCtx("Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:154","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleWarnNoCtx() {
	setup()
	WarnNoCtx("Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:161","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleErrorNoCtx() {
	setup()
	ErrorNoCtx("Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:168","ts":"2000-01-01T00:00:00.000000000Z"}
}

func Example_danglingKey() {
	setup()
	Info(context.Background(), "Hello, world!", "myDanglingKey")
	// Output:
	// {"level":"error","msg":"Ignored key without a value.","caller":"log_test.go:175","ts":"2000-01-01T00:00:00.000000000Z","ignored":"myDanglingKey"}
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:175","ts":"2000-01-01T00:00:00.000000000Z"}
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
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:183","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:184","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:198","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:199","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:213","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:214","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
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
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:228","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:229","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar","abc":123,"time":"2000-01-02T00:00:00.000000000Z"}
}

func ExampleDebugf_logger() {
	setup()
	logger.Debugf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:243","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfof_logger() {
	setup()
	logger.Infof(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:250","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnf_logger() {
	setup()
	logger.Warnf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:257","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorf_logger() {
	setup()
	logger.Errorf(ctx, "Hello, %s!", "world")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:264","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugv_logger() {
	setup()
	logger.Debugv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:271","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleInfov_logger() {
	setup()
	logger.Infov(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:278","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleWarnv_logger() {
	setup()
	logger.Warnv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:285","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleErrorv_logger() {
	setup()
	logger.Errorv(ctx, 0, "Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:292","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func ExampleDebugNoCtx_logger() {
	setup()
	logger.DebugNoCtx("Hello, world!")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:299","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleInfoNoCtx_logger() {
	setup()
	logger.InfoNoCtx("Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:306","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleWarnNoCtx_logger() {
	setup()
	logger.WarnNoCtx("Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:313","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleErrorNoCtx_logger() {
	setup()
	logger.ErrorNoCtx("Hello, world!")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:320","ts":"2000-01-01T00:00:00.000000000Z"}
}

func Example_level() {
	setup()
	logger = NewLogger(Config{
		OutputPaths: []string{"stdout"},
		Level:       InfoLevel,
	})
	logger.now = func() time.Time {
		return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	Debug(ctx, "Hello, world!")
	Info(ctx, "Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:335","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}

func Example_envVarLogLevel() {
	old := os.Getenv(LOG_LEVEL_ENV_VAR)
	os.Setenv(LOG_LEVEL_ENV_VAR, "WARN")
	setup()
	Info(ctx, "Hello, world!")
	Warn(ctx, "Hello, world!")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:345","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
	os.Setenv(LOG_LEVEL_ENV_VAR, old)
}

func Example_defaultFields() {
	setup()
	logger = NewLoggerWithDefaultFields(Config{
		OutputPaths: []string{"stdout"},
		Level:       InfoLevel,
	}, []interface{}{"foo", "bar"})
	logger.now = func() time.Time {
		return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	logger.Info(ctx, "Hello, world!")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:360","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar"}
}

func Example_defaultFieldsDanglingKey() {
	setup()
	logger = NewLoggerWithDefaultFields(Config{
		OutputPaths: []string{"stdout"},
		Level:       InfoLevel,
	}, []interface{}{"foo", "bar", "foobar"})
	logger.now = func() time.Time {
		return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	logger.Info(ctx, "Hello, world!")
	// Output:
	// {"level":"error","msg":"defaultFields contains a key without a value.","ignored":"foobar"}
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:374","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","foo":"bar"}
}

func ExampleDebugfNoCtx() {
	setup()
	DebugfNoCtx("Hello, %s!", "world")
	// Output:
	// {"level":"debug","msg":"Hello, world!","caller":"log_test.go:382","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleInfofNoCtx() {
	setup()
	InfofNoCtx("Hello, %s!", "world")
	// Output:
	// {"level":"info","msg":"Hello, world!","caller":"log_test.go:389","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleWarnfNoCtx() {
	setup()
	WarnfNoCtx("Hello, %s!", "world")
	// Output:
	// {"level":"warn","msg":"Hello, world!","caller":"log_test.go:396","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleErrorfNoCtx() {
	setup()
	ErrorfNoCtx("Hello, %s!", "world")
	// Output:
	// {"level":"error","msg":"Hello, world!","caller":"log_test.go:403","ts":"2000-01-01T00:00:00.000000000Z"}
}

func ExampleSetLoggerConfig() {
	setup()
	SetLoggerConfig(Config{
		OutputPaths: TestConfig.OutputPaths,
		Level:       InfoLevel,
	})
	logger.now = func() time.Time {
		return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	Debug(ctx, "Hello, world!")
	Info(ctx, "Goodbye, world!")
	// Output:
	// {"level":"info","msg":"Goodbye, world!","caller":"log_test.go:418","ts":"2000-01-01T00:00:00.000000000Z","requestID":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
}
