package log

import (
	"context"
	"fmt"
)

var logger = NewLogger()

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
