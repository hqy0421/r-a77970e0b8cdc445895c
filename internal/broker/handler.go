package broker

import (
	"context"
	"log/slog"
	"time"

	"github.com/adexaja/shoebox/internal/retry"
	"github.com/adexaja/shoebox/internal/storage"
)

// HandlerFunc is the function signature of a registered handler. It is
// distinct from the public shoebox.HandlerFunc so the broker doesn't
// import the public package (which would create an import cycle); the
// public Queue converts at registration time.
type HandlerFunc func(ctx context.Context, m storage.Message) error

// handler bundles a registered handler with its options and the
// pre-composed middleware chain.
type handler struct {
	fn    HandlerFunc
	opts  HandlerOptions
	chain HandlerFunc
}

// HandlerOptions configures a single handler. Mirrors the public
// shoebox.HandlerOptions; the public Queue converts at registration time.
type HandlerOptions struct {
	MaxRetries int
	Backoff    retry.Backoff
	Timeout    time.Duration

	// MaxRetryAfter caps how long a retryAfterHint may defer the next
	// attempt. Zero means no cap. It does not affect Backoff decisions.
	MaxRetryAfter time.Duration
}

// retryAfterHint is implemented by handler errors that request a specific
// wait before the next attempt — for example when a webhook target answers
// with an HTTP Retry-After header. The broker uses the delay in place of the
// handler's Backoff for that retry, capped by HandlerOptions.MaxRetryAfter.
// The attempt still consumes the retry budget: it is counted in Attempts and
// takes the normal dead-letter path once MaxRetries is exhausted.
type retryAfterHint interface {
	RetryAfter() time.Duration
}

// logSink is the subset of *slog.Logger the broker uses. It exists so
// tests can swap in a no-op logger if they don't want to assert against
// log output.
type logSink interface {
	InfoContext(ctx context.Context, msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	DebugContext(ctx context.Context, msg string, args ...any)
}

var _ logSink = (*slog.Logger)(nil)
