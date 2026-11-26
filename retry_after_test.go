package shoebox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/adexaja/shoebox/internal/retry"
)

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		name  string
		value string
		want  time.Duration
		ok    bool
	}{
		{"empty", "", 0, false},
		{"delta seconds", "120", 120 * time.Second, true},
		{"whitespace trimmed", "  5 ", 5 * time.Second, true},
		{"zero", "0", 0, false},
		{"negative", "-30", 0, false},
		{"garbage", "soon", 0, false},
		{"future date", now.Add(90 * time.Second).Format(http.TimeFormat), 90 * time.Second, true},
		{"past date", now.Add(-90 * time.Second).Format(http.TimeFormat), 0, false},
		{"overflow clamped", "99999999999999999", time.Duration(1<<63-1) / time.Second * time.Second, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseRetryAfter(c.value, now)
			if ok != c.ok {
				t.Fatalf("ok = %v, want %v", ok, c.ok)
			}
			if ok && c.want != 0 {
				// Allow tiny skew for date formatting truncation to seconds.
				if diff := got - c.want; diff < -time.Second || diff > time.Second {
					t.Fatalf("delay = %v, want ~%v", got, c.want)
				}
			}
		})
	}
}

func TestRetryAfterError_Wrapping(t *testing.T) {
	base := errors.New("boom")
	err := WithRetryAfter(base, 5*time.Second)
	if !errors.Is(err, base) {
		t.Fatal("wrapped error should satisfy errors.Is for the cause")
	}
	var rae *RetryAfterError
	if !errors.As(err, &rae) {
		t.Fatal("errors.As should find *RetryAfterError")
	}
	if rae.RetryAfter() != 5*time.Second {
		t.Fatalf("RetryAfter() = %v, want 5s", rae.RetryAfter())
	}
}

func TestWithRetryAfter_NonPositiveIsNoop(t *testing.T) {
	base := errors.New("boom")
	if got := WithRetryAfter(base, 0); got != base {
		t.Fatal("zero delay should return the original error unchanged")
	}
	if got := WithRetryAfter(base, -time.Second); got != base {
		t.Fatal("negative delay should return the original error unchanged")
	}
}

// TestWebhookHandler_RetryAfterHint verifies a non-2xx response with a valid
// Retry-After header surfaces the wait as a RetryAfterError.
func TestWebhookHandler_RetryAfterHint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	h := WebhookHandler(srv.URL, nil)
	err := h(context.Background(), Message{ID: "m", Queue: "q", Payload: []byte("x")})
	if err == nil {
		t.Fatal("expected an error for a 429 response")
	}
	var rae *RetryAfterError
	if !errors.As(err, &rae) {
		t.Fatalf("error %v is not a *RetryAfterError", err)
	}
	if rae.RetryAfter() != 7*time.Second {
		t.Fatalf("hint = %v, want 7s", rae.RetryAfter())
	}
}

// TestWebhookHandler_NoRetryAfterHeader verifies a plain non-2xx response
// carries no hint, so the broker's backoff applies.
func TestWebhookHandler_NoRetryAfterHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	h := WebhookHandler(srv.URL, nil)
	err := h(context.Background(), Message{ID: "m", Queue: "q", Payload: []byte("x")})
	if err == nil {
		t.Fatal("expected an error")
	}
	var rae *RetryAfterError
	if errors.As(err, &rae) {
		t.Fatalf("unexpected RetryAfterError for response without header: %v", err)
	}
}

// TestWebhookHandler_InvalidRetryAfterHeader verifies an unparseable header is
// ignored rather than surfaced.
func TestWebhookHandler_InvalidRetryAfterHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "not-a-date")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	h := WebhookHandler(srv.URL, nil)
	err := h(context.Background(), Message{ID: "m", Queue: "q", Payload: []byte("x")})
	if err == nil {
		t.Fatal("expected an error")
	}
	var rae *RetryAfterError
	if errors.As(err, &rae) {
		t.Fatalf("unexpected RetryAfterError for invalid header: %v", err)
	}
}

// TestWebhookHandler_SuccessIgnoresRetryAfter verifies a successful response
// with a Retry-After header is still acked (no requeue).
func TestWebhookHandler_SuccessIgnoresRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := WebhookHandler(srv.URL, nil)
	if err := h(context.Background(), Message{ID: "m", Queue: "q", Payload: []byte("x")}); err != nil {
		t.Fatalf("expected nil error on 2xx, got %v", err)
	}
}

// TestWebhook_EndToEnd_RetryAfterRespected verifies the broker schedules a
// hinted retry at the requested time (not the fast backoff) while other
// messages are still delivered during the wait.
func TestWebhook_EndToEnd_RetryAfterRespected(t *testing.T) {
	var mu sync.Mutex
	var slowHits []time.Time
	var fastDelivered int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		switch string(body) {
		case "slow":
			slowHits = append(slowHits, time.Now())
			if len(slowHits) == 1 {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
		case "fast":
			fastDelivered++
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	q, err := New(Options{
		Storage: Memory,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = q.Shutdown(ctx)
	}()

	q.Handle("hooks", WebhookHandler(srv.URL, nil), HandlerOptions{
		MaxRetries: 1,
		// 10ms backoff would retry near-instantly; the 1s hint must win.
		Backoff: retry.Constant(10 * time.Millisecond),
	})

	start := time.Now()
	if err := q.Enqueue("hooks", []byte("slow")); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue("hooks", []byte("fast")); err != nil {
		t.Fatal(err)
	}

	// Wait until the slow message has been retried successfully.
	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		done := len(slowHits) >= 2
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("timed out; slow hits=%d fast=%d", len(slowHits), fastDelivered)
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	// The gap between the failed attempt and its retry must be ~1s, well above
	// the 10ms backoff.
	if gap := slowHits[1].Sub(slowHits[0]); gap < 700*time.Millisecond {
		t.Fatalf("retry after %v, want ~1s from the Retry-After hint", gap)
	}
	// The unrelated message must have been delivered without waiting for the
	// hinted retry.
	if fastDelivered != 1 {
		t.Fatalf("fast deliveries = %d, want 1", fastDelivered)
	}
	if fast := slowHits[1].Sub(start); fast < 700*time.Millisecond {
		t.Fatalf("slow retry completed after %v; hint not honored", fast)
	}
}
