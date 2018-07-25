package loginterfaces

import (
	"context"
)

type Logger interface {
	Debug(ctx context.Context, msg string, keysAndValues ...interface{})
	Debugf(ctx context.Context, fs string, args ...interface{})
	Debugv(ctx context.Context, skip int, msg string, keysAndValues ...interface{})
	DebugNoCtx(msg string, keysAndValues ...interface{})
	Info(ctx context.Context, msg string, keysAndValues ...interface{})
	Infof(ctx context.Context, fs string, args ...interface{})
	Infov(ctx context.Context, skip int, msg string, keysAndValues ...interface{})
	InfoNoCtx(msg string, keysAndValues ...interface{})
	Warn(ctx context.Context, msg string, keysAndValues ...interface{})
	Warnf(ctx context.Context, fs string, args ...interface{})
	Warnv(ctx context.Context, skip int, msg string, keysAndValues ...interface{})
	WarnNoCtx(msg string, keysAndValues ...interface{})
	Error(ctx context.Context, msg string, keysAndValues ...interface{})
	Errorf(ctx context.Context, fs string, args ...interface{})
	Errorv(ctx context.Context, skip int, msg string, keysAndValues ...interface{})
	ErrorNoCtx(msg string, keysAndValues ...interface{})
}
