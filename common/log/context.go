package log

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"

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

// WithNewRequestID generates a random uuid value, sets it for the RequestIDContextKey
// key in the context, and returns context and new uuid.
func WithNewRequestID(ctx context.Context) (context.Context, uuid.UUID) {
	requestID := uuid.New()
	return context.WithValue(ctx, RequestIDContextKey, requestID), requestID
}

// WithGinRequestID sets a random identifier on the gin Context. The context should
// be passed to the methods in this package to prefix logs with the identifier.
func WithGinRequestID(ctx *gin.Context) {
	id, err := randomID(16)
	if err != nil {
		_ = ctx.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	ctx.Set(RequestIDContextKey, id)
}

// RandomID generates a random padded base 64 URL encoded string of at least 4*n/3 length.
// Recommend minimum of n=16 for 128 bits of randomness if you need practical uniqueness.
func randomID(n int) (string, error) {
	rnd := make([]byte, n)
	if _, err := rand.Read(rnd); err != nil {
		return "", fmt.Errorf("failed to get random bytes: %s", err)
	}
	return base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(rnd), nil
}
