package discord

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pratherbytecraft/discord-scraper-go/internal/ratelimit"
	"github.com/pratherbytecraft/discord-scraper-go/pkg/snowflake"
)

// BufferPool provides reusable 128 KiB byte buffers to eliminate network heap allocations.
// Specifically tailored to fit within L2/L3 cache architectures without triggering GC churn.
var BufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 128*1024)
		return &b
	},
}

// Client is a high-performance Discord REST client with integrated rate-limiting,
// connection pooling, retries, and 429 handling.
type Client struct {
	token      string
	httpClient *http.Client
	limiter    *ratelimit.Limiter
	baseURL    string
	userAgent  string
}

// ClientOption configures the client.
type ClientOption func(*Client)

// WithBaseURL overrides the API base (useful for testing).
func WithBaseURL(u string) ClientOption {
	return func(c *Client) { c.baseURL = strings.TrimRight(u, "/") }
}

// WithHTTPClient overrides the underlying http.Client.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) { c.httpClient = hc }
}

// WithTargetRPS sets ban-safe global target requests per second (30-40 recommended, max 45).
func WithTargetRPS(rps int) ClientOption {
	return func(c *Client) {
		if c.limiter != nil {
			c.limiter.SetTargetRPS(rps)
		}
	}
}

// WithLimiter injects a custom limiter (for testing).
func WithLimiter(l *ratelimit.Limiter) ClientOption {
	return func(c *Client) { c.limiter = l }
}

// NewClient creates a Discord client. Token may be "Bot <token>" or raw user token.
func NewClient(token string, opts ...ClientOption) *Client {
	c := &Client{
		token:   token,
		baseURL: "https://discord.com/api/v10",
		userAgent: "Mozilla/5.0 (X11; Linux x86_64; rv:17.0) Gecko/20121202 Firefox/17.0 Iceweasel/17.0.1",
		limiter: ratelimit.New(),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   10 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:        128,
				MaxIdleConnsPerHost: 32,
				MaxConnsPerHost:     0, // unlimited, limiter manages dispatch
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  false,
				ForceAttemptHTTP2:   true,
			},
		},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// apiError is returned for non-2xx non-429 responses.
type apiError struct {
	Status int
	Body   string
	URL    string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("discord api %d on %s: %s", e.Status, e.URL, e.Body)
}

// rateLimitBody is the JSON shape Discord returns for 429.
type rateLimitBody struct {
	Message    string  `json:"message"`
	RetryAfter float64 `json:"retry_after"`
	Global     bool    `json:"global"`
}

// do executes an HTTP request with full rate-limit dance.
// bucketFallback is used when X-RateLimit-Bucket header is absent (e.g. before first response).
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	var bodyBytes []byte
	var err error
	if body != nil {
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
	}

	fullURL := c.baseURL + path
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}

	// Route-derived bucket fallback: method + base route (strip IDs).
	// We approximate by using the path with IDs replaced? Simple: path prefix up to 3 segments.
	bucketFallback := method + ":" + pathBucketKey(path)

	maxRetries := 12
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := c.limiter.Wait(ctx, bucketFallback); err != nil {
			return err
		}

		var reqBody io.Reader
		if bodyBytes != nil {
			reqBody = bytes.NewReader(bodyBytes)
		}
		req, err := http.NewRequestWithContext(ctx, method, fullURL, reqBody)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", c.token)
		req.Header.Set("User-Agent", c.userAgent)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Network error: retry with backoff if context not cancelled
			if ctx.Err() != nil {
				return ctx.Err()
			}
			backoff := time.Duration(200*(1<<attempt)) * time.Millisecond
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
			select {
			case <-time.After(backoff):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Drain body for keep-alive reuse helper
		handle := func() error {
			defer resp.Body.Close()

			bucketKey := ratelimit.BucketKey(resp.Header, bucketFallback)
			c.limiter.UpdateFromHeaders(bucketKey, resp.Header)
			// Also update fallback mapping so future waits cover this bucket
			if bucketKey != bucketFallback {
				c.limiter.UpdateFromHeaders(bucketFallback, resp.Header)
			}

			switch resp.StatusCode {
			case http.StatusOK, http.StatusCreated, http.StatusNoContent:
				if out != nil && resp.StatusCode != http.StatusNoContent {
					// Some endpoints return 200 with empty body; handle gracefully.
					data, err := io.ReadAll(resp.Body)
					if err != nil {
						return err
					}
					if len(data) == 0 {
						return nil
					}
					if err := json.Unmarshal(data, out); err != nil {
						return fmt.Errorf("unmarshal %s: %w (body: %s)", fullURL, err, string(data[:min(512, len(data))]))
					}
				} else {
					io.Copy(io.Discard, resp.Body)
				}
				return nil

			case http.StatusTooManyRequests:
				// Parse retry_after
				data, _ := io.ReadAll(resp.Body)
				var rl rateLimitBody
				_ = json.Unmarshal(data, &rl)
				// Fallback to header if json missing
				if rl.RetryAfter == 0 {
					if v := resp.Header.Get("Retry-After"); v != "" {
						if f, err := strconv.ParseFloat(v, 64); err == nil {
							rl.RetryAfter = f
						}
					}
				}
				if rl.RetryAfter == 0 {
					rl.RetryAfter = 1.0
				}
				bucketKey429 := ratelimit.BucketKey(resp.Header, bucketFallback)
				c.limiter.Register429(bucketKey429, rl.RetryAfter, rl.Global)
				// Also register on fallback key for next Wait
				if bucketKey429 != bucketFallback {
					c.limiter.Register429(bucketFallback, rl.RetryAfter, rl.Global)
				}
				// Sleep already handled via limiter's global/bucket until, but we also wait
				// via limiter.Wait loop; continue to retry.
				return fmt.Errorf("429")
			default:
				// 5xx: retry, 4xx: fail fast (except 429 handled above)
				if resp.StatusCode >= 500 && resp.StatusCode < 600 && attempt < maxRetries-1 {
					io.Copy(io.Discard, resp.Body)
					backoff := time.Duration(300*(1<<attempt)) * time.Millisecond
					if backoff > 5*time.Second {
						backoff = 5 * time.Second
					}
					select {
					case <-time.After(backoff):
						return fmt.Errorf("5xx retry")
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
				return &apiError{Status: resp.StatusCode, Body: string(data), URL: fullURL}
			}
		}

		err = handle()
		if err == nil {
			return nil
		}
		// Check if it's a retriable sentinel
		if err.Error() == "429" || err.Error() == "5xx retry" {
			continue
		}
		// Check typed apiError for 5xx that we already decided to retry: handled above.
		return err
	}
	return fmt.Errorf("max retries exceeded for %s %s", method, fullURL)
}

// doRaw executes an HTTP request and reads the response directly into a pooled buffer.
// Returns the raw byte slice, a release function to return the buffer to BufferPool, and any error.
func (c *Client) doRaw(ctx context.Context, method, path string, query url.Values) ([]byte, func(), error) {
	fullURL := c.baseURL + path
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}

	bucketFallback := method + ":" + pathBucketKey(path)
	maxRetries := 12

	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := c.limiter.Wait(ctx, bucketFallback); err != nil {
			return nil, nil, err
		}

		req, err := http.NewRequestWithContext(ctx, method, fullURL, nil)
		if err != nil {
			return nil, nil, err
		}
		req.Header.Set("Authorization", c.token)
		req.Header.Set("User-Agent", c.userAgent)
		req.Header.Set("Accept", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			backoff := time.Duration(200*(1<<attempt)) * time.Millisecond
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
			select {
			case <-time.After(backoff):
				continue
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		}

		bucketKey := ratelimit.BucketKey(resp.Header, bucketFallback)
		c.limiter.UpdateFromHeaders(bucketKey, resp.Header)
		if bucketKey != bucketFallback {
			c.limiter.UpdateFromHeaders(bucketFallback, resp.Header)
		}

		switch resp.StatusCode {
		case http.StatusOK, http.StatusCreated:
			bufPtr := BufferPool.Get().(*[]byte)
			buf := (*bufPtr)[:0]

			tmp := make([]byte, 32*1024)
			var readErr error
			for {
				n, rerr := resp.Body.Read(tmp)
				if n > 0 {
					buf = append(buf, tmp[:n]...)
				}
				if rerr != nil {
					if rerr != io.EOF {
						readErr = rerr
					}
					break
				}
			}
			resp.Body.Close()
			if readErr != nil {
				*bufPtr = buf[:0]
				BufferPool.Put(bufPtr)
				return nil, nil, readErr
			}
			release := func() {
				*bufPtr = buf[:0]
				BufferPool.Put(bufPtr)
			}
			return buf, release, nil

		case http.StatusNoContent:
			resp.Body.Close()
			return nil, func() {}, nil

		case http.StatusTooManyRequests:
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var rl rateLimitBody
			_ = json.Unmarshal(data, &rl)
			if rl.RetryAfter == 0 {
				if v := resp.Header.Get("Retry-After"); v != "" {
					if f, err := strconv.ParseFloat(v, 64); err == nil {
						rl.RetryAfter = f
					}
				}
			}
			if rl.RetryAfter == 0 {
				rl.RetryAfter = 1.0
			}
			c.limiter.Register429(bucketKey, rl.RetryAfter, rl.Global)
			if bucketKey != bucketFallback {
				c.limiter.Register429(bucketFallback, rl.RetryAfter, rl.Global)
			}
			continue

		default:
			if resp.StatusCode >= 500 && resp.StatusCode < 600 && attempt < maxRetries-1 {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				backoff := time.Duration(300*(1<<attempt)) * time.Millisecond
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
				select {
				case <-time.After(backoff):
					continue
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
			data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return nil, nil, &apiError{Status: resp.StatusCode, Body: string(data), URL: fullURL}
		}
	}
	return nil, nil, fmt.Errorf("max retries exceeded for %s %s", method, fullURL)
}

// pathBucketKey returns a coarse route key (helps before receiving bucket header).
// In Discord API, major parameters (channel_id, guild_id) form independent rate-limit buckets.
func pathBucketKey(path string) string {
	parts := strings.Split(path, "/")
	if len(parts) >= 4 && parts[1] == "channels" && parts[3] == "messages" {
		if len(parts) == 4 {
			return path // e.g. /channels/123/messages
		}
		return "/channels/" + parts[2] + "/messages/:id"
	}
	if len(parts) >= 4 && parts[1] == "guilds" && parts[3] == "channels" {
		return path
	}
	for i, p := range parts {
		if i > 2 && len(p) >= 16 && isSnowflakeLike(p) {
			parts[i] = ":id"
		}
	}
	return strings.Join(parts, "/")
}

func isSnowflakeLike(s string) bool {
	if len(s) < 17 || len(s) > 20 {
		return false
	}
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// --- Helper for search snowflake generation (mirrors Discrub) ---

// SnowflakeFromTime delegates to pkg/snowflake.
func SnowflakeFromTime(t time.Time) string { return snowflake.FromTime(t) }

// Auth test helper.
func (c *Client) FetchSelf(ctx context.Context) (*User, error) {
	var u User
	if err := c.do(ctx, http.MethodGet, "/users/@me", nil, nil, &u); err != nil {
		return nil, err
	}
	return &u, nil
}
