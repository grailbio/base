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
	// DebugLevel logs are typically voluminous.
	DebugLevel = zapcore.DebugLevel
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
		DebugLevel: coreLogger.Debugw,
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
		Level:       zap.NewAtomicLevelAt(zap.DebugLevel),
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
	//if ctx != nil {
	//	if requestID := v23.GetRequestID(vcontext.FromGoContext(ctx)); requestID != uuid.Nil {
	//		defaultFields = append(defaultFields, "requestID", requestID)
	//	}
	//}
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

// Debug logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Debug(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Debugv(ctx, 1, msg, keysAndValues...)
}

// Info logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Info(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Infov(ctx, 1, msg, keysAndValues...)
}

// Warn logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Warn(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Warnv(ctx, 1, msg, keysAndValues...)
}

// Error logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Error(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Errorv(ctx, 1, msg, keysAndValues...)
}

// Debugf uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Debugf(ctx context.Context, fs string, args ...interface{}) {
	Debugv(ctx, 1, fmt.Sprintf(fs, args...))
}

// Infof uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Infof(ctx context.Context, fs string, args ...interface{}) {
	Infov(ctx, 1, fmt.Sprintf(fs, args...))
}

// Warnf uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Warnf(ctx context.Context, fs string, args ...interface{}) {
	Warnv(ctx, 1, fmt.Sprintf(fs, args...))
}

// Errorf uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Errorf(ctx context.Context, fs string, args ...interface{}) {
	Errorv(ctx, 1, fmt.Sprintf(fs, args...))
}

// Debugv logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller is skipped by skip.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Debugv(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	log(ctx, DebugLevel, skip, msg, keysAndValues)
}

// Infov logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller is skipped by skip.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Infov(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	log(ctx, InfoLevel, skip, msg, keysAndValues)
}

// Warnv logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller is skipped by skip.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Warnv(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	log(ctx, WarnLevel, skip, msg, keysAndValues)
}

// Errorv logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller is skipped by skip.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Errorv(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	log(ctx, ErrorLevel, skip, msg, keysAndValues)
}

// DebugNoCtx logs a message and variadic key-value pairs.
func DebugNoCtx(msg string, keysAndValues ...interface{}) {
	Debugv(nil, 1, msg, keysAndValues...) // nolint: staticcheck
}

// InfoNoCtx logs a message and variadic key-value pairs.
func InfoNoCtx(msg string, keysAndValues ...interface{}) {
	Infov(nil, 1, msg, keysAndValues...) // nolint: staticcheck
}

// WarnNoCtx logs a message and variadic key-value pairs.
func WarnNoCtx(msg string, keysAndValues ...interface{}) {
	Warnv(nil, 1, msg, keysAndValues...) // nolint: staticcheck
}

// ErrorNoCtx logs a message and variadic key-value pairs.
func ErrorNoCtx(msg string, keysAndValues ...interface{}) {
	Errorv(nil, 1, msg, keysAndValues...) // nolint: staticcheck
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
