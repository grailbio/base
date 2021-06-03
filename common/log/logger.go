package log

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	v23 "v.io/v23"
	vcontext "v.io/v23/context"
)

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
	// FatalLevel logs a message, then calls os.Exit(1).
	FatalLevel = zapcore.FatalLevel
	// RFC3339TrailingNano is RFC3339 format with trailing nanoseconds precision.
	RFC3339TrailingNano = "2006-01-02T15:04:05.000000000Z07:00"
	// LOG_LEVEL_ENV_VAR is the environment variable name used to set logging level.
	LOG_LEVEL_ENV_VAR = "LOG_LEVEL"
)

// contextFields is a list of context key-value pairs to be logged.
// Key is the name of the field.
// Value is the context key.
var contextFields = map[string]interface{}{
	"requestID": RequestIDContextKey,
}

var logLvls = map[string]zapcore.Level{
	"debug": DebugLevel,
	"DEBUG": DebugLevel,
	"info":  InfoLevel,
	"INFO":  InfoLevel,
	"warn":  WarnLevel,
	"WARN":  WarnLevel,
	"error": ErrorLevel,
	"ERROR": ErrorLevel,
	"fatal": FatalLevel,
	"FATAL": FatalLevel,
}

type Logger struct {
	coreLogger    *zap.SugaredLogger
	defaultFields []interface{}
	levelToLogger map[zapcore.Level]func(msg string, keysAndValues ...interface{})
	now           func() time.Time
}

type Config struct {
	OutputPaths []string
	// note: setting the environment variable LOG_LEVEL will override Config.Level
	Level zapcore.Level
}

func setDefaultLogLevelsMap(logger *Logger) *Logger {
	logger.levelToLogger = map[zapcore.Level]func(msg string, keysAndValues ...interface{}){
		DebugLevel: logger.coreLogger.Debugw,
		InfoLevel:  logger.coreLogger.Infow,
		WarnLevel:  logger.coreLogger.Warnw,
		ErrorLevel: logger.coreLogger.Errorw,
		FatalLevel: logger.coreLogger.Fatalw,
	}
	return logger
}

func NewLogger(config Config) *Logger {
	return NewLoggerWithDefaultFields(config, []interface{}{})
}

// NewLogger creates a new logger instance.
// defaultFields is a list of key-value pairs to be included in every log message.
func NewLoggerWithDefaultFields(config Config, defaultFields []interface{}) *Logger {
	l := Logger{
		coreLogger:    mustBuildLogger(config, zap.AddCallerSkip(2)),
		defaultFields: defaultFields,
		now:           time.Now,
	}

	return setDefaultLogLevelsMap(&l)
}

// NewLoggerFromCore allows the caller to pass in a zap.SugaredLogger into the logger.
// This allows one to make unit test assertions about logs.
func NewLoggerFromCore(lager *zap.SugaredLogger) *Logger {
	l := Logger{
		coreLogger: lager,
		now:        time.Now,
	}

	return setDefaultLogLevelsMap(&l)
}

func (l *Logger) log(ctx context.Context, level zapcore.Level, callerSkip int, msg string, keysAndValues []interface{}) {
	t := l.now()
	// Add default fields
	keysAndValues = append(keysAndValues, l.defaultFields...)
	// If there is a dangling key (i.e. odd length keysAndValues), log an error and then
	// drop the dangling key and log original message.
	if len(keysAndValues)%2 != 0 {
		danglingKey := keysAndValues[len(keysAndValues)-1]
		keysAndValues = keysAndValues[:len(keysAndValues)-1]
		errLog := withDefaultFields(ctx, callerSkip, t, "ignored", danglingKey)
		logErr := l.levelToLogger[ErrorLevel]
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
	logLevel := l.levelToLogger[level]
	logLevel(msg, keysAndValues...)
}

// Debug logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Debug(ctx context.Context, msg string, keysAndValues ...interface{}) {
	l.Debugv(ctx, 1, msg, keysAndValues...)
}

// Debugf uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Debugf(ctx context.Context, fs string, args ...interface{}) {
	l.Debugv(ctx, 1, fmt.Sprintf(fs, args...))
}

// Debugv logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller stack field is skipped by skip levels.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Debugv(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	l.log(ctx, DebugLevel, skip, msg, keysAndValues)
}

// DebugNoCtx logs a message and variadic key-value pairs.
func (l *Logger) DebugNoCtx(msg string, keysAndValues ...interface{}) {
	l.Debugv(context.Background(), 1, msg, keysAndValues...)
}

// Info logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Info(ctx context.Context, msg string, keysAndValues ...interface{}) {
	l.Infov(ctx, 1, msg, keysAndValues...)
}

// Infof uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Infof(ctx context.Context, fs string, args ...interface{}) {
	l.Infov(ctx, 1, fmt.Sprintf(fs, args...))
}

// Infov logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller stack field is skipped by skip levels.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Infov(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	l.log(ctx, InfoLevel, skip, msg, keysAndValues)
}

// InfoNoCtx logs a message and variadic key-value pairs.
func (l *Logger) InfoNoCtx(msg string, keysAndValues ...interface{}) {
	l.Infov(context.Background(), 1, msg, keysAndValues...)
}

// Warn logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Warn(ctx context.Context, msg string, keysAndValues ...interface{}) {
	l.Warnv(ctx, 1, msg, keysAndValues...)
}

// Warnf uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Warnf(ctx context.Context, fs string, args ...interface{}) {
	l.Warnv(ctx, 1, fmt.Sprintf(fs, args...))
}

// Warnv logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller stack field is skipped by skip levels.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Warnv(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	l.log(ctx, WarnLevel, skip, msg, keysAndValues)
}

// WarnNoCtx logs a message and variadic key-value pairs.
func (l *Logger) WarnNoCtx(msg string, keysAndValues ...interface{}) {
	l.Warnv(context.Background(), 1, msg, keysAndValues...)
}

// Fatal logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Fatal(ctx context.Context, msg string, keysAndValues ...interface{}) {
	l.Fatalv(ctx, 1, msg, keysAndValues...)
}

// Fatalf uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Fatalf(ctx context.Context, fs string, args ...interface{}) {
	l.Fatalv(ctx, 1, fmt.Sprintf(fs, args...))
}

// Fatalv logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller stack field is skipped by skip levels.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Fatalv(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	l.log(ctx, FatalLevel, skip, msg, keysAndValues)
}

// FatalNoCtx logs a message and variadic key-value pairs.
func (l *Logger) FatalNoCtx(msg string, keysAndValues ...interface{}) {
	l.Fatalv(context.Background(), 1, msg, keysAndValues...)
}

// Error logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Error(ctx context.Context, msg string, keysAndValues ...interface{}) {
	l.Errorv(ctx, 1, msg, keysAndValues...)
}

// Errorf uses fmt.Sprintf to log a templated message and the key-value pairs defined in contextFields from ctx.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Errorf(ctx context.Context, fs string, args ...interface{}) {
	l.Errorv(ctx, 1, fmt.Sprintf(fs, args...))
}

// Errorv logs a message, the key-value pairs defined in contextFields from ctx, and variadic key-value pairs.
// Caller stack field is skipped by skip levels.
// If ctx is nil, all fields from contextFields will be omitted.
// If ctx does not contain a key in contextFields, that field will be omitted.
func (l *Logger) Errorv(ctx context.Context, skip int, msg string, keysAndValues ...interface{}) {
	l.log(ctx, ErrorLevel, skip, msg, keysAndValues)
}

// ErrorNoCtx logs a message and variadic key-value pairs.
func (l *Logger) ErrorNoCtx(msg string, keysAndValues ...interface{}) {
	l.Errorv(context.Background(), 1, msg, keysAndValues...)
}

// rfc3339TrailingNanoTimeEncoder serializes a time.Time to an RFC3339-formatted string
// with trailing nanosecond precision.
func rfc3339TrailingNanoTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format(RFC3339TrailingNano))
}

func mustBuildLogger(config Config, opts ...zap.Option) *zap.SugaredLogger {
	zapLogger, err := newConfig(config).Build(opts...)
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
func newConfig(override Config) zap.Config {
	// Default config
	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.DebugLevel),
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         "json",
		EncoderConfig:    newEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	// config overrides
	if override.OutputPaths != nil {
		config.OutputPaths = override.OutputPaths
	}
	if override.Level != zapcore.DebugLevel {
		config.Level = zap.NewAtomicLevelAt(override.Level)
	}
	// LOG_LEVEL environment variable override
	// Note: setting the environment variable LOG_LEVEL will override Config.Level
	if logLvl, ok := logLvls[os.Getenv(LOG_LEVEL_ENV_VAR)]; ok {
		config.Level = zap.NewAtomicLevelAt(logLvl)
	}
	return config
}

func withDefaultFields(ctx context.Context, callerSkip int, t time.Time,
	keysAndValues ...interface{}) []interface{} {
	defaultFields := []interface{}{
		"caller", getCaller(callerSkip),
		"ts", t,
	}
	if ctx != nil {
		if vctx, ok := ctx.(*vcontext.T); ok {
			if requestID := v23.GetRequestID(vctx); requestID != uuid.Nil {
				defaultFields = append(defaultFields, "v23RequestID", requestID)
			}
		}
	}
	return append(defaultFields, keysAndValues...)
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
	parts := strings.Split(frame.File, "/")
	file := parts[len(parts)-1]
	return fmt.Sprintf("%s:%d", file, frame.Line)
}
