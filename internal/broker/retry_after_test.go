package broker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adexaja/shoebox/internal/retry"
	"github.com/adexaja/shoebox/internal/storage"
)

// hintErr is a handler error carrying an explicit retry wait.
type hintErr struct{ d time.Duration }

func (e hintErr) Error() string             { return "please wait" }
func (e hintErr) RetryAfter() time.Duration { return e.d }

// retryRecorder wraps a Storage and records the ScheduledAt of every message
// passed to Retry, so tests can observe the computed retry delay without
// racing the dispatcher's dequeue/poll loop.
type retryRecorder struct {
	storage.Storage
	mu        sync.Mutex
	scheduled []time.Time
}

func (r *retryRecorder) Retry(ctx context.Context, queue string, msg storage.Message, err error) error {
	r.mu.Lock()
	r.scheduled = append(r.scheduled, msg.ScheduledAt)
	r.mu.Unlock()
	return r.Storage.Retry(ctx, queue, msg, err)
}

// waitForRetry blocks until the recorder has seen at least n retries and
// returns the recorded ScheduledAt of the n-th one.
func (r *retryRecorder) waitForRetry(t *testing.T, n int) time.Time {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		if len(r.scheduled) >= n {
			got := r.scheduled[n-1]
			r.mu.Unlock()
			return got
		}
		r.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("saw fewer than %d retries", n)
	return time.Time{}
}

// newRetryHarness builds a broker over a retryRecorder and enqueues one
// message on "jobs" using fn as the handler.
func newRetryHarness(t *testing.T, fn HandlerFunc, opts HandlerOptions) (*retryRecorder, *Broker) {
	t.Helper()
	rec := &retryRecorder{Storage: storage.NewMemory()}
	b := quietBroker(t, rec, 1)
	b.Register("jobs", fn, opts)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	})
	if err := b.Enqueue(context.Background(), "jobs", []byte("x"), EnqueueOpts{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return rec, b
}

// TestRetryDelay_HonorsHint verifies a handler-supplied wait replaces the
// backoff for that retry.
func TestRetryDelay_HonorsHint(t *testing.T) {
	const wait = 400 * time.Millisecond
	var before atomic.Int64
	rec, _ := newRetryHarness(t, func(context.Context, storage.Message) error {
		before.Store(time.Now().UnixNano())
		return hintErr{d: wait}
	}, HandlerOptions{MaxRetries: 1, Backoff: retry.Constant(time.Millisecond)})

	scheduled := rec.waitForRetry(t, 1)
	got := scheduled.Sub(time.Unix(0, before.Load()))
	if got < wait/2 || got > wait+time.Second {
		t.Fatalf("retry scheduled %v out, want ~%v", got, wait)
	}
}

// TestRetryDelay_CapsOversizedHint verifies MaxRetryAfter truncates a hint
// beyond the configured ceiling.
func TestRetryDelay_CapsOversizedHint(t *testing.T) {
	const capDelay = 150 * time.Millisecond
	var before atomic.Int64
	rec, _ := newRetryHarness(t, func(context.Context, storage.Message) error {
		before.Store(time.Now().UnixNano())
		return hintErr{d: time.Hour}
	}, HandlerOptions{
		MaxRetries:    1,
		Backoff:       retry.Constant(time.Millisecond),
		MaxRetryAfter: capDelay,
	})

	scheduled := rec.waitForRetry(t, 1)
	got := scheduled.Sub(time.Unix(0, before.Load()))
	if got < capDelay/2 || got > capDelay+time.Second {
		t.Fatalf("retry scheduled %v out, want it capped near %v", got, capDelay)
	}
}

// TestRetryDelay_FallsBackWithoutHint verifies a plain (hint-less) error keeps
// the configured backoff, and that MaxRetryAfter does not constrain it.
func TestRetryDelay_FallsBackWithoutHint(t *testing.T) {
	const backoff = 300 * time.Millisecond
	var before atomic.Int64
	rec, _ := newRetryHarness(t, func(context.Context, storage.Message) error {
		before.Store(time.Now().UnixNano())
		return errors.New("plain failure")
	}, HandlerOptions{
		MaxRetries:    1,
		Backoff:       retry.Constant(backoff),
		MaxRetryAfter: 50 * time.Millisecond,
	})

	scheduled := rec.waitForRetry(t, 1)
	got := scheduled.Sub(time.Unix(0, before.Load()))
	if got < backoff/2 || got > backoff+time.Second {
		t.Fatalf("retry scheduled %v out, want ~%v backoff", got, backoff)
	}
}

// TestRetryDelay_ZeroHintUsesBackoff verifies a non-positive hint is ignored,
// so the backoff decides.
func TestRetryDelay_ZeroHintUsesBackoff(t *testing.T) {
	const backoff = 250 * time.Millisecond
	var before atomic.Int64
	rec, _ := newRetryHarness(t, func(context.Context, storage.Message) error {
		before.Store(time.Now().UnixNano())
		return hintErr{d: 0}
	}, HandlerOptions{MaxRetries: 1, Backoff: retry.Constant(backoff)})

	scheduled := rec.waitForRetry(t, 1)
	got := scheduled.Sub(time.Unix(0, before.Load()))
	if got < backoff/2 || got > backoff+time.Second {
		t.Fatalf("retry scheduled %v out, want ~%v backoff for a zero hint", got, backoff)
	}
}

// TestRetryDelay_HintStillConsumesBudget verifies a hint changes timing only:
// every hinted retry consumes the MaxRetries budget and the message still
// dead-letters when spent.
func TestRetryDelay_HintStillConsumesBudget(t *testing.T) {
	store := storage.NewMemory()
	b := quietBroker(t, store, 1)
	defer func() { _ = b.Shutdown(context.Background()) }()

	var attempts atomic.Int64
	b.Register("jobs", func(context.Context, storage.Message) error {
		attempts.Add(1)
		return hintErr{d: 10 * time.Millisecond}
	}, HandlerOptions{MaxRetries: 2, Backoff: retry.Constant(time.Millisecond)})

	if err := b.Enqueue(context.Background(), "jobs", []byte("x"), EnqueueOpts{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := store.Stats(context.Background(), "jobs")
		if err != nil {
			t.Fatalf("stats: %v", err)
		}
		if stats.Dead > 0 {
			// Initial attempt + 2 retries = 3 deliveries, then dead-letter.
			if got := attempts.Load(); got != 3 {
				t.Fatalf("attempts = %d, want 3 (initial + MaxRetries)", got)
			}
			if stats.Depth != 0 {
				t.Fatalf("depth = %d after dead-letter, want 0", stats.Depth)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("message never dead-lettered after exhausting budget; attempts=%d", attempts.Load())
}

// TestRetryAfter_PersistedWaitSurvivesRestart verifies a hinted ScheduledAt is
// written to storage, so reopening the database does not deliver the message
// before the requested time.
func TestRetryAfter_PersistedWaitSurvivesRestart(t *testing.T) {
	path := t.TempDir() + "/retry.db"
	ctx := context.Background()

	store, err := storage.NewSQLite(ctx, path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	b := quietBroker(t, store, 1)
	b.Register("jobs", func(context.Context, storage.Message) error {
		return hintErr{d: 600 * time.Millisecond}
	}, HandlerOptions{MaxRetries: 1, Backoff: retry.Constant(time.Millisecond)})
	if err := b.Enqueue(ctx, "jobs", []byte("x"), EnqueueOpts{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Wait until the retry is persisted, then shut down before it comes due.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := store.Stats(ctx, "jobs")
		if err != nil {
			t.Fatalf("stats: %v", err)
		}
		if stats.Depth > 0 && stats.Retries > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// Reopen well before the hinted time; the message must still be pending
	// and not yet due.
	store2, err := storage.NewSQLite(ctx, path)
	if err != nil {
		t.Fatalf("reopen sqlite: %v", err)
	}
	defer func() { _ = store2.Close() }()

	msgs, err := store2.Dequeue(ctx, "jobs", 1)
	if err == nil && len(msgs) > 0 {
		t.Fatalf("message delivered before its hinted retry time (scheduled at %v)", msgs[0].ScheduledAt)
	}
	if err != nil && !errors.Is(err, storage.ErrEmpty) {
		t.Fatalf("dequeue after restart: %v", err)
	}
}
