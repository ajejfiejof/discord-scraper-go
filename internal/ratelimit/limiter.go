package ratelimit

import (
	"context"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Limiter implements Discord's rate-limit handling (global + per-bucket).
// Ban-safe enhancements:
//   - Global token-bucket via nextAllowed (targetRPS, default 32 req/s, 0.64 of Discord 50)
//   - Per-bucket predictive throttling when Remaining <= 1
//   - Jitter on sleeps to avoid thundering herd
//   - Handles 429 global vs bucket, adds 100-300ms safety buffer
type Limiter struct {
	mu          sync.Mutex
	globalUntil time.Time
	buckets     map[string]*bucket

	// Global token bucket
	targetRPS     int           // e.g. 32
	globalInterval time.Duration // 1/targetRPS
	nextAllowed   time.Time

	// Safety: when bucket remaining <=1, we delay until reset rather than bursting.
}

type bucket struct {
	remaining int
	resetAt   time.Time
	limit     int
}

// New creates a ban-safe limiter at 32 req/s (conservative vs Discord 50).
func New() *Limiter {
	return NewWithTarget(32)
}

// NewWithTarget creates limiter with custom target RPS (ban-safe). 30-35 recommended.
func NewWithTarget(targetRPS int) *Limiter {
	if targetRPS <= 0 {
		targetRPS = 32
	}
	if targetRPS > 45 {
		targetRPS = 45 // cap to avoid accidental ban
	}
	return &Limiter{
		buckets:        make(map[string]*bucket),
		targetRPS:      targetRPS,
		globalInterval: time.Second / time.Duration(targetRPS),
		nextAllowed:    time.Now(),
	}
}

// SetTargetRPS updates global target (thread-safe).
func (l *Limiter) SetTargetRPS(rps int) {
	if rps <= 0 {
		return
	}
	if rps > 45 {
		rps = 45
	}
	l.mu.Lock()
	l.targetRPS = rps
	l.globalInterval = time.Second / time.Duration(rps)
	l.mu.Unlock()
}

// Wait blocks until the request is allowed under global and bucket limits.
func (l *Limiter) Wait(ctx context.Context, bucketKey string) error {
	for {
		l.mu.Lock()
		now := time.Now()

		// 1) Global 429 lock
		if now.Before(l.globalUntil) {
			sleep := time.Until(l.globalUntil)
			// jitter 0-50ms
			sleep += time.Duration(rand.Int63n(50)) * time.Millisecond
			l.mu.Unlock()
			select {
			case <-time.After(sleep):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// 2) Per-bucket 429 / predictive throttle
		if b, ok := l.buckets[bucketKey]; ok {
			if b.remaining == 0 && now.Before(b.resetAt) {
				sleep := time.Until(b.resetAt) + time.Duration(80+rand.Int63n(120))*time.Millisecond
				l.mu.Unlock()
				select {
				case <-time.After(sleep):
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			// Predictive: if remaining ==1 and reset is >500ms away, add small yield to avoid bursting to 0
			if b.remaining == 1 && now.Before(b.resetAt) {
				// If we're about to exhaust, add a fraction of reset window
				remainingWindow := time.Until(b.resetAt)
				if remainingWindow > 400*time.Millisecond {
					sleep := remainingWindow / time.Duration(b.limit+1)
					if sleep > 20*time.Millisecond && sleep < 500*time.Millisecond {
						l.mu.Unlock()
						select {
						case <-time.After(sleep):
							continue
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
			}
		}

		// 3) Global token bucket (ban-safe pacing)
		if l.globalInterval > 0 {
			if now.Before(l.nextAllowed) {
				sleep := time.Until(l.nextAllowed)
				l.mu.Unlock()
				select {
				case <-time.After(sleep):
					// loop again to re-check bucket after sleep
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			// Reserve slot
			// Add tiny jitter 0-5ms to desync goroutines
			jitter := time.Duration(rand.Int63n(5)) * time.Millisecond
			l.nextAllowed = now.Add(l.globalInterval + jitter)
			// If nextAllowed is in past (idle), snap to now+interval
			if l.nextAllowed.Before(now.Add(l.globalInterval)) {
				l.nextAllowed = now.Add(l.globalInterval)
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

	if remainingStr == "" && limitStr == "" && resetAfterStr == "" {
		// Also check plain Reset as fallback, but if all empty, no update
		if h.Get("X-RateLimit-Reset") == "" {
			return
		}
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
	}
	if v, err := strconv.ParseFloat(resetAfterStr, 64); err == nil {
		// Add small safety margin: Discord says resetAfter is until window reset; we add 30ms
		extra := 30 * time.Millisecond
		b.resetAt = time.Now().Add(time.Duration(v*float64(time.Second)) + extra)
		// Clamp absurdly long resets (e.g. >30s) to 5s to avoid stuck bucket on buggy header
		if v > 30 {
			b.resetAt = time.Now().Add(5 * time.Second)
		}
		// Ensure monotonic: if resetAt is in past, don't use
		if b.resetAt.Before(time.Now()) {
			b.resetAt = time.Now().Add(time.Duration(v * float64(time.Second)))
		}
		_ = math.Max // keep import
	}

	if resetAfterStr == "" {
		if resetStr := h.Get("X-RateLimit-Reset"); resetStr != "" {
			if f, err := strconv.ParseFloat(resetStr, 64); err == nil {
				b.resetAt = time.Unix(int64(f), int64((f-float64(int64(f)))*1e9))
				if b.resetAt.Before(time.Now()) {
					b.resetAt = time.Now().Add(time.Second)
				}
			}
		}
	}

	// If we just learned remaining is high, ensure global pacing isn't over-conservative
	// (no action needed — global token bucket already paces)
}

// Register429 records a 429 response.
func (l *Limiter) Register429(bucketKey string, retryAfter float64, isGlobal bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	d := time.Duration(retryAfter * float64(time.Second))
	if d <= 0 {
		d = time.Second
	}
	// Safety buffer: +150-300ms jitter + 10% of retryAfter
	d += time.Duration(150+rand.Int63n(150)) * time.Millisecond
	d += time.Duration(float64(d) * 0.1)
	if isGlobal {
		l.globalUntil = time.Now().Add(d)
		// Also push global token bucket forward
		if l.nextAllowed.Before(l.globalUntil) {
			l.nextAllowed = l.globalUntil
		}
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

// Stats returns a snapshot for debugging (globalUntil, bucket count).
func (l *Limiter) Stats() (globalUntil time.Time, buckets int, nextAllowed time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.globalUntil, len(l.buckets), l.nextAllowed
}
