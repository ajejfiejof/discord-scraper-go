package forum

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Client is a Discourse scraper with realistic browser headers + forum_session cookie.
// Reuses the same AATB limiter concept but Discourse is far more permissive than Discord (no 50 RPS cap).
type Client struct {
	baseURL    string
	cookie     string
	httpClient *http.Client
	userAgent  string
}

func NewClient(baseURL, cookie string) *Client {
	if baseURL == "" {
		baseURL = "https://example.com"
	}
	return &Client{
		baseURL: baseURL,
		cookie:  cookie,
		userAgent: "Mozilla/5.0 (X11; Linux x86_64; rv:158.0) Gecko/20100101 Firefox/158.0",
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (c *Client) do(ctx context.Context, path string, query url.Values, out any) error {
	fullURL := c.baseURL + path
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}
	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", c.userAgent)
		req.Header.Set("Accept", "application/json, text/javascript, */*; q=0.01")
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
		req.Header.Set("Referer", c.baseURL+"/")
		req.Header.Set("X-Requested-With", "XMLHttpRequest")
		req.Header.Set("Connection", "keep-alive")
		if c.cookie != "" {
			req.Header.Set("Cookie", c.cookie)
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			time.Sleep(time.Duration(300*(1<<attempt)) * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 429 {
			retryAfter := 2 * time.Second
			if v := resp.Header.Get("Retry-After"); v != "" {
				if sec, err := strconv.Atoi(v); err == nil {
					retryAfter = time.Duration(sec) * time.Second
				} else if f, err := strconv.ParseFloat(v, 64); err == nil {
					retryAfter = time.Duration(f * float64(time.Second))
				}
			}
			retryAfter += time.Duration(500+attempt*500) * time.Millisecond
			select {
			case <-time.After(retryAfter):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("forum api %d on %s: %s", resp.StatusCode, fullURL, string(body[:min(2048, len(body))]))
		}
		if out != nil {
			if err := json.Unmarshal(body, out); err != nil {
				return fmt.Errorf("unmarshal %s: %w", fullURL, err)
			}
		}
		// Be nice: 400ms delay for Discourse (was 100ms, got 429 at 16 workers)
		time.Sleep(400 * time.Millisecond)
		return nil
	}
	return fmt.Errorf("forum max retries exceeded for %s", fullURL)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (c *Client) FetchCategories(ctx context.Context) (*CategoriesResponse, error) {
	var out CategoriesResponse
	if err := c.do(ctx, "/categories.json", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) FetchTop(ctx context.Context, period string, page int) (*TopicListResponse, error) {
	q := url.Values{}
	if period != "" {
		q.Set("period", period)
	}
	if page > 0 {
		q.Set("page", strconv.Itoa(page))
	}
	var out TopicListResponse
	if err := c.do(ctx, "/top.json", q, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) FetchLatest(ctx context.Context, page int) (*TopicListResponse, error) {
	q := url.Values{}
	if page > 0 {
		q.Set("page", strconv.Itoa(page))
	}
	var out TopicListResponse
	if err := c.do(ctx, "/latest.json", q, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) FetchCategory(ctx context.Context, categoryID, page int) (*TopicListResponse, error) {
	q := url.Values{}
	if page > 0 {
		q.Set("page", strconv.Itoa(page))
	}
	var out TopicListResponse
	// Discourse: /c/:id.json or /c/:slug/:id.json — use /c/:id.json
	if err := c.do(ctx, fmt.Sprintf("/c/%d.json", categoryID), q, &out); err != nil {
		return nil, err
	}
	// Some Discourse versions nest under topic_list, others under category topic list; normalize
	return &out, nil
}

func (c *Client) FetchTopic(ctx context.Context, topicID int) (*TopicResponse, error) {
	var out TopicResponse
	if err := c.do(ctx, fmt.Sprintf("/t/%d.json", topicID), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) FetchPosts(ctx context.Context, topicID, page int) (*PostsResponse, error) {
	q := url.Values{}
	if page > 1 {
		q.Set("page", strconv.Itoa(page))
	}
	var out PostsResponse
	// Discourse paginates posts via ?page= or via /t/:id/:post_number.json
	if err := c.do(ctx, fmt.Sprintf("/t/%d.json", topicID), q, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
