package log

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	RequestIDContextKey = "requestID"
)

// WithRequestID sets the uuid value for the RequestIDContextKey key in the context.
func WithRequestID(ctx context.Context, requestID uuid.UUID) context.Context {
	return context.WithValue(ctx, RequestIDContextKey, requestID)
}

// WithGinRequestID creates a uuid that is set as a string on the gin Context and as
// a uuid on the regular-flavor Request context that it wraps. The context should
// be passed to the methods in this package to prefix logs with the identifier.
func WithGinRequestID(ctx *gin.Context) {
	requuid := uuid.New()
	uuidStr := requuid.String()
	ctx.Set(RequestIDContextKey, uuidStr)
	// TODO: ideally we'd pass the  X-Amzn-Trace-Id header from our ALB, but we're not using ALBs yet.
	ctx.Request = ctx.Request.WithContext(WithRequestID(ctx.Request.Context(), requuid))
}
