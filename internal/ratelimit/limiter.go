package ratelimit

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Limiter implements Discord's rate-limit handling (global + per-bucket).
// It is safe for concurrent use and optimized for high throughput.
type Limiter struct {
	mu            sync.Mutex
	globalUntil   time.Time
	buckets       map[string]*bucket
	// minInterval enforces a tiny gap to avoid bursting too hard (0 = disabled).
	// Discord allows ~50 req/s globally; we stay conservative and let headers drive.
}

type bucket struct {
	remaining int
	resetAt   time.Time
	limit     int
}

// New creates a Limiter.
func New() *Limiter {
	return &Limiter{
		buckets: make(map[string]*bucket),
	}
}

// Wait blocks until the request is allowed under global and bucket limits.
// bucketKey should be derived from X-RateLimit-Bucket or route path if header missing.
func (l *Limiter) Wait(ctx context.Context, bucketKey string) error {
	for {
		l.mu.Lock()
		now := time.Now()

		// Global lock
		if now.Before(l.globalUntil) {
			sleep := time.Until(l.globalUntil)
			l.mu.Unlock()
			select {
			case <-time.After(sleep):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		b, ok := l.buckets[bucketKey]
		if ok && b.remaining == 0 && now.Before(b.resetAt) {
			sleep := time.Until(b.resetAt)
			l.mu.Unlock()
			select {
			case <-time.After(sleep):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		l.mu.Unlock()
		return nil
	}
}

// UpdateFromHeaders records rate-limit state from response headers.
func (l *Limiter) UpdateFromHeaders(bucketKey string, h http.Header) {
	remainingStr := h.Get("X-RateLimit-Remaining")
	limitStr := h.Get("X-RateLimit-Limit")
	resetAfterStr := h.Get("X-RateLimit-Reset-After")
	// Some responses use Retry-After for 429; not handled here.

	if remainingStr == "" && limitStr == "" && resetAfterStr == "" {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[bucketKey]
	if !ok {
		b = &bucket{}
		l.buckets[bucketKey] = b
	}
	if v, err := strconv.Atoi(limitStr); err == nil {
		b.limit = v
	}
	if v, err := strconv.Atoi(remainingStr); err == nil {
		b.remaining = v
	} else if remainingStr == "" {
		// If header missing, assume unlimited (don't throttle).
	}
	if v, err := strconv.ParseFloat(resetAfterStr, 64); err == nil {
		b.resetAt = time.Now().Add(time.Duration(v * float64(time.Second)))
	}

	// Also check plain Reset (unix timestamp) as fallback
	if resetAfterStr == "" {
		if resetStr := h.Get("X-RateLimit-Reset"); resetStr != "" {
			if f, err := strconv.ParseFloat(resetStr, 64); err == nil {
				b.resetAt = time.Unix(int64(f), int64((f-float64(int64(f)))*1e9))
				if b.resetAt.Before(time.Now()) {
					// If timestamp in past (second precision), add 1s buffer
					b.resetAt = time.Now().Add(time.Second)
				}
			}
		}
	}
}

// Register429 records a 429 response.
func (l *Limiter) Register429(bucketKey string, retryAfter float64, isGlobal bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	d := time.Duration(retryAfter * float64(time.Second))
	if d <= 0 {
		d = time.Second
	}
	// Add 100ms jitter buffer
	d += 100 * time.Millisecond
	if isGlobal {
		l.globalUntil = time.Now().Add(d)
	} else {
		b, ok := l.buckets[bucketKey]
		if !ok {
			b = &bucket{}
			l.buckets[bucketKey] = b
		}
		b.remaining = 0
		b.resetAt = time.Now().Add(d)
	}
}

// BucketKey extracts bucket header or falls back to route.
func BucketKey(h http.Header, fallback string) string {
	if v := h.Get("X-RateLimit-Bucket"); v != "" {
		return v
	}
	return fallback
}
