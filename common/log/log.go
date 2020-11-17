package log

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// RFC3339TrailingNano is RFC3339 format with trailing nanoseconds precision.
const RFC3339TrailingNano = "2006-01-02T15:04:05.000000000Z07:00"

// contextFields is a list of context key-value pairs to be logged.
// Key is the name of the field.
// Value is the context key.
var contextFields = map[string]interface{}{
	"requestID": RequestIDContextKey,
}

const (
	// InfoLevel is the default logging priority.
	InfoLevel = zapcore.InfoLevel
	// WarnLevel logs are more important than Info, but don't need individual human review.
	WarnLevel = zapcore.WarnLevel
	// ErrorLevel logs are high-priority.
	// Applications running smoothly shouldn't generate any error-level logs.
	ErrorLevel = zapcore.ErrorLevel
)

var (
	coreLogger    = mustBuildLogger(zap.AddCallerSkip(2))
	levelToLogger = map[zapcore.Level]func(msg string, keysAndValues ...interface{}){
		InfoLevel:  coreLogger.Infow,
		WarnLevel:  coreLogger.Warnw,
		ErrorLevel: coreLogger.Errorw,
	}
	now = time.Now
)

// rfc3339TrailingNanoTimeEncoder serializes a time.Time to an RFC3339-formatted string
// with trailing nanosecond precision.
func rfc3339TrailingNanoTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format(RFC3339TrailingNano))
}

func mustBuildLogger(opts ...zap.Option) *zap.SugaredLogger {
	zapLogger, err := newConfig().Build(opts...)
	if err != nil {
		panic(err)
	}
	return zapLogger.Sugar()
}

// newEncoderConfig is similar to Zap's NewProductionConfig with a few modifications
// to better fit our needs.
func newEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		LevelKey:       "level",
		NameKey:        "logger",
		MessageKey:     "msg",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     rfc3339TrailingNanoTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

// newConfig is similar to Zap's NewProductionConfig with a few modifications
// to better fit our needs.
func newConfig() zap.Config {
	return zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         "json",
		EncoderConfig:    newEncoderConfig(),
		OutputPaths:      []string{"stdout", "stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
}

func withDefaultFields(ctx context.Context, callerSkip int, t time.Time,
	keysAndValues ...interface{}) []interface{} {
	defaultFields := []interface{}{
		"caller", getCaller(callerSkip),
		"ts", t,
	}
	// TODO(noah): Uncomment after v.io upgrade.
	// if requestID := v23.GetRequestID(vcontext.FromGoContext(ctx)); requestID != uuid.Nil {
	// 	defaultFields = append(defaultFields, "requestID", requestID)
	// }
	return append(defaultFields, keysAndValues...)
}

func log(ctx context.Context, level zapcore.Level, callerSkip int, msg string, keysAndValues []interface{}) {
	t := now()
	// If there is a dangling key (i.e. odd length keysAndValues), log an error and then
	// drop the dangling key and log original message.
	if len(keysAndValues)%2 != 0 {
		danglingKey := keysAndValues[len(keysAndValues)-1]
		keysAndValues = keysAndValues[:len(keysAndValues)-1]
		errLog := withDefaultFields(ctx, callerSkip, t, "ignored", danglingKey)
		logErr := levelToLogger[ErrorLevel]
		logErr("Ignored key without a value.", errLog...)
	}
	// Add caller and timestamp fields
	prefix := withDefaultFields(ctx, callerSkip, t)
	// Add context logged fields
	if ctx != nil {
		for k, v := range contextFields {
			if ctxVal := ctx.Value(v); ctxVal != nil {
				prefix = append(prefix, k, ctxVal)
			}
		}
	}
	keysAndValues = append(prefix, keysAndValues...)
	// Log at the appropriate level
	logLevel := levelToLogger[level]
	logLevel(msg, keysAndValues...)
}

// Info logs a message, certain values from ctx, and variadic key-value pairs.
func Info(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Infov(ctx, 1, msg, keysAndValues...)
}

// Warn logs a message, certain values from ctx, and variadic key-value pairs.
func Warn(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Warnv(ctx, 1, msg, keysAndValues...)
}

// Error logs a message, certain values from ctx, and variadic key-value pairs.
func Error(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Errorv(ctx, 1, msg, keysAndValues...)
}

// Infof uses fmt.Sprintf to log a templated message.
func Infof(ctx context.Context, fs string, args ...interface{}) {
	Infov(ctx, 1, fmt.Sprintf(fs, args...))
}

// Warnf uses fmt.Sprintf to log a templated message.
func Warnf(ctx context.Context, fs string, args ...interface{}) {
	Warnv(ctx, 1, fmt.Sprintf(fs, args...))
}

// Errorf uses fmt.Sprintf to log a templated message.
func Errorf(ctx context.Context, fs string, args ...interface{}) {
	Errorv(ctx, 1, fmt.Sprintf(fs, args...))
}

// Infov logs a message, certain values from ctx, and variadic key-value pairs.
// Caller is skipped by skip.
func Infov(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	log(ctx, InfoLevel, skip, msg, keysAndValues)
}

// Warnv logs a message, certain values from ctx, and variadic key-value pairs.
// Caller is skipped by skip.
func Warnv(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	log(ctx, WarnLevel, skip, msg, keysAndValues)
}

// Errorv logs a message, certain values from ctx, and variadic key-value pairs.
// Caller is skipped by skip.
func Errorv(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	log(ctx, ErrorLevel, skip, msg, keysAndValues)
}

func getCaller(skip int) string {
	skipOffset := 5
	pc := make([]uintptr, 1)
	numFrames := runtime.Callers(skip+skipOffset, pc)
	if numFrames < 1 {
		return ""
	}
	frame, _ := runtime.CallersFrames(pc).Next()
	if frame.PC == 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", frame.File, frame.Line)
}
