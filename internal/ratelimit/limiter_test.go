package ratelimit

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestLimiterWaitNoBlocking(t *testing.T) {
	l := New()
	ctx := context.Background()
	start := time.Now()
	if err := l.Wait(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("wait should be immediate when no limits")
	}
}

func TestLimiter429Global(t *testing.T) {
	l := New()
	l.Register429("bucket1", 0.1, true)
	ctx := context.Background()
	start := time.Now()
	if err := l.Wait(ctx, "any"); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed < 80*time.Millisecond {
		t.Fatalf("expected to block ~100ms, got %v", elapsed)
	}
}

func TestLimiterUpdateFromHeaders(t *testing.T) {
	l := New()
	h := http.Header{}
	h.Set("X-RateLimit-Remaining", "0")
	h.Set("X-RateLimit-Limit", "5")
	h.Set("X-RateLimit-Reset-After", "0.2")
	l.UpdateFromHeaders("my-bucket", h)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := l.Wait(ctx, "my-bucket")
	if err == nil {
		t.Fatal("expected timeout when bucket is exhausted")
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("should have waited until timeout")
	}
}
