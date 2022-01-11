package log

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	// Best practices for avoiding key collisions in context say this shouldn't be a string type.
	// Gin forces us to use a string for their context, so choose a name that is unlikely to collide with anything else.
	RequestIDContextKey = "grail_logger_request_id"
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
	if _, ok := ctx.Get(RequestIDContextKey); ok {
		return // Avoid overwriting the original value in case this middleware is invoked twice
	}
	ctx.Set(RequestIDContextKey, uuidStr)
	// TODO: ideally we'd pass the  X-Amzn-Trace-Id header from our ALB, but we're not using ALBs yet.
	ctx.Request = ctx.Request.WithContext(WithRequestID(ctx.Request.Context(), requuid))
}
