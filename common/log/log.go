package log

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

var logger = NewLogger(Config{Level: DebugLevel})

// Instantiate a new logger and assign any key-value pair to addedInfo field in logger to log additional
// information specific to service
func GetNewLoggerWithDefaultFields(addedInfo ...interface{}) *Logger {
	return NewLoggerWithDefaultFields(Config{Level: DebugLevel}, addedInfo)
}

// Debug logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Debug(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Debugv(ctx, 1, msg, keysAndValues...)
}

// Debugf uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Debugf(ctx context.Context, fs string, args ...interface{}) {
	Debugv(ctx, 1, fmt.Sprintf(fs, args...))
}

// Debugv logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller is skipped by skip.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Debugv(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	logger.Debugv(ctx, skip+1, msg, keysAndValues...)
}

// DebugNoCtx logs a message and variadic key-value pairs.
func DebugNoCtx(msg string, keysAndValues ...interface{}) {
	// context.Background() is a singleton and gets initialized once
	Debugv(context.Background(), 1, msg, keysAndValues...)
}

// DebugfNoCtx uses fmt.Sprintf to log a templated message.
func DebugfNoCtx(fs string, args ...interface{}) {
	Debugv(context.Background(), 1, fmt.Sprintf(fs, args...))
}

// Info logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Info(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Infov(ctx, 1, msg, keysAndValues...)
}

// Infof uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Infof(ctx context.Context, fs string, args ...interface{}) {
	Infov(ctx, 1, fmt.Sprintf(fs, args...))
}

// Infov logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller is skipped by skip.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Infov(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	logger.Infov(ctx, skip+1, msg, keysAndValues...)
}

// InfoNoCtx logs a message and variadic key-value pairs.
func InfoNoCtx(msg string, keysAndValues ...interface{}) {
	// context.Background() is a singleton and gets initialized once
	Infov(context.Background(), 1, msg, keysAndValues...)
}

// InfofNoCtx uses fmt.Sprintf to log a templated message.
func InfofNoCtx(fs string, args ...interface{}) {
	Infov(context.Background(), 1, fmt.Sprintf(fs, args...))
}

// Warn logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Warn(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Warnv(ctx, 1, msg, keysAndValues...)
}

// Warnf uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Warnf(ctx context.Context, fs string, args ...interface{}) {
	Warnv(ctx, 1, fmt.Sprintf(fs, args...))
}

// Warnv logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller is skipped by skip.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Warnv(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	logger.Warnv(ctx, skip+1, msg, keysAndValues...)
}

// WarnNoCtx logs a message and variadic key-value pairs.
func WarnNoCtx(msg string, keysAndValues ...interface{}) {
	// context.Background() is a singleton and gets initialized once
	Warnv(context.Background(), 1, msg, keysAndValues...)
}

// WarnfNoCtx uses fmt.Sprintf to log a templated message.
func WarnfNoCtx(fs string, args ...interface{}) {
	Warnv(context.Background(), 1, fmt.Sprintf(fs, args...))
}

// Fatal logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Fatal(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Fatalv(ctx, 1, msg, keysAndValues...)
}

// Fatalf uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Fatalf(ctx context.Context, fs string, args ...interface{}) {
	Fatalv(ctx, 1, fmt.Sprintf(fs, args...))
}

// Fatalv logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller is skipped by skip.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Fatalv(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	logger.Fatalv(ctx, skip+1, msg, keysAndValues...)
}

// FatalNoCtx logs a message and variadic key-value pairs.
func FatalNoCtx(msg string, keysAndValues ...interface{}) {
	// context.Background() is a singleton and gets initialized once
	Fatalv(context.Background(), 1, msg, keysAndValues...)
}

// FatalfNoCtx uses fmt.Sprintf to log a templated message.
func FatalfNoCtx(fs string, args ...interface{}) {
	Fatalv(context.Background(), 1, fmt.Sprintf(fs, args...))
}

// Error logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Error(ctx context.Context, msg string, keysAndValues ...interface{}) {
	Errorv(ctx, 1, msg, keysAndValues...)
}

// Errorf uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Errorf(ctx context.Context, fs string, args ...interface{}) {
	Errorv(ctx, 1, fmt.Sprintf(fs, args...))
}

// Errorv logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller is skipped by skip.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func Errorv(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	logger.Errorv(ctx, skip+1, msg, keysAndValues...)
}

// ErrorNoCtx logs a message and variadic key-value pairs.
func ErrorNoCtx(msg string, keysAndValues ...interface{}) {
	// context.Background() is a singleton and gets initialized once
	Errorv(context.Background(), 1, msg, keysAndValues...)
}

// ErrorfNoCtx uses fmt.Sprintf to log a templated message.
func ErrorfNoCtx(fs string, args ...interface{}) {
	Errorv(context.Background(), 1, fmt.Sprintf(fs, args...))
}

// Error logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
// Returns the message (without keysAndValues) as a string for convenience.
func ErrorAndReturn(ctx context.Context, msg string, keysAndValues ...interface{}) string {
	return ErrorvAndReturn(ctx, 1, msg, keysAndValues...)
}

// Errorf uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
// Returns the formatted message as a string for convenience.
func ErrorfAndReturn(ctx context.Context, fs string, args ...interface{}) string {
	return ErrorvAndReturn(ctx, 1, fmt.Sprintf(fs, args...))
}

// Errorv logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller is skipped by skip.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
// Returns the message (without keysAndValues) as a string for convenience.
func ErrorvAndReturn(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) string {
	logger.Errorv(ctx, skip+1, msg, keysAndValues...)
	return msg
}

// ErrorNoCtx logs a message and variadic key-value pairs.
// Returns the message (without keysAndValues) as a string for convenience.
func ErrorNoCtxAndReturn(msg string, keysAndValues ...interface{}) string {
	// context.Background() is a singleton and gets initialized once
	return ErrorvAndReturn(context.Background(), 1, msg, keysAndValues...)
}

func InjectTestLogger(testLogger *zap.SugaredLogger) {
	logger = NewLoggerFromCore(testLogger)
}
