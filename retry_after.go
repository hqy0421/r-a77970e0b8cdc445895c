package shoebox

import (
	"fmt"
	"time"
)

// RetryAfterError is a handler error that asks the queue to wait a specific
// amount of time before the next delivery attempt. The broker uses Delay in
// place of the handler's Backoff for that retry; the attempt still counts
// against MaxRetries and takes the normal dead-letter path when the budget is
// exhausted.
//
// WebhookHandler returns one of these when a target responds with a valid
// Retry-After header. Custom handlers may return one too:
//
//	return shoebox.WithRetryAfter(err, 30*time.Second)
type RetryAfterError struct {
	// Delay is how long to wait before the next attempt.
	Delay time.Duration

	// Err is the underlying failure, if any. It is returned by Unwrap and
	// shown by Error.
	Err error
}

// Error implements error.
func (e *RetryAfterError) Error() string {
	if e == nil {
		return "shoebox: retry after"
	}
	if e.Err != nil {
		return fmt.Sprintf("%v (retry after %s)", e.Err, e.Delay)
	}
	return fmt.Sprintf("shoebox: retry after %s", e.Delay)
}

// Unwrap returns the underlying error, so errors.Is and errors.As continue to
// see through the hint.
func (e *RetryAfterError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// RetryAfter returns the requested delay. It satisfies the broker's retry-hint
// interface.
func (e *RetryAfterError) RetryAfter() time.Duration {
	if e == nil {
		return 0
	}
	return e.Delay
}

// WithRetryAfter wraps err with a wait hint. A non-positive delay leaves err
// unchanged, so the handler's configured Backoff still applies.
func WithRetryAfter(err error, d time.Duration) error {
	if d <= 0 {
		return err
	}
	return &RetryAfterError{Delay: d, Err: err}
}
