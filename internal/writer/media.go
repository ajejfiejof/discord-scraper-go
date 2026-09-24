package writer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
)

// MediaBudget tracks downloaded bytes against a 20GB cap.
// When exceeded, further downloads are skipped and a warning is logged.
// Textual messages are always scraped; this only gates attachments.
type MediaBudget struct {
	limitBytes int64
	usedBytes  atomic.Int64
	client     *http.Client
}

func NewMediaBudget(limitBytes int64) *MediaBudget {
	if limitBytes <= 0 {
		return nil // disabled
	}
	return &MediaBudget{
		limitBytes: limitBytes,
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     90 * 1e9,
			},
		},
	}
}

func (m *MediaBudget) Used() int64 { return m.usedBytes.Load() }
func (m *MediaBudget) Limit() int64 { return m.limitBytes }
func (m *MediaBudget) Remaining() int64 { return m.limitBytes - m.usedBytes.Load() }
func (m *MediaBudget) Exceeded() bool { return m.usedBytes.Load() >= m.limitBytes }

// TryReserve atomically reserves n bytes if within budget. Returns true if reserved.
func (m *MediaBudget) TryReserve(n int64) bool {
	if m == nil {
		return false
	}
	for {
		used := m.usedBytes.Load()
		if used+n > m.limitBytes {
			return false
		}
		if m.usedBytes.CompareAndSwap(used, used+n) {
			return true
		}
	}
}

// AddUsed records actual bytes written (for unknown size downloads).
func (m *MediaBudget) AddUsed(n int64) int64 { return m.usedBytes.Add(n) }

// Download fetches url to dest path, respecting budget. Caller should check size header first.
func (m *MediaBudget) Download(ctx context.Context, url, dest string, sizeHint int64) (int64, error) {
	if m != nil && m.Exceeded() {
		return 0, fmt.Errorf("media budget exceeded (%d/%d bytes)", m.Used(), m.Limit())
	}
	if m != nil && sizeHint > 0 && !m.TryReserve(sizeHint) {
		return 0, fmt.Errorf("media budget would exceed: need %d, remaining %d", sizeHint, m.Remaining())
	}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		if m != nil && sizeHint > 0 {
			m.usedBytes.Add(-sizeHint) // rollback reserve
		}
		return 0, err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		if m != nil && sizeHint > 0 {
			m.usedBytes.Add(-sizeHint)
		}
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		if m != nil && sizeHint > 0 {
			m.usedBytes.Add(-sizeHint)
		}
		return 0, fmt.Errorf("cdn %d for %s", resp.StatusCode, url)
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return 0, err
	}
	f, err := os.Create(dest)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := io.Copy(f, resp.Body)
	if err != nil {
		return n, err
	}
	// If sizeHint was 0 (unknown), add actual now; if we reserved, adjust delta
	if m != nil {
		if sizeHint == 0 {
			if !m.TryReserve(n) {
				// Over budget after download — remove file and rollback
				_ = os.Remove(dest)
				return n, fmt.Errorf("media budget exceeded after download (%d bytes)", m.Remaining())
			}
		} else if n != sizeHint {
			// adjust reservation to actual
			delta := n - sizeHint
			if delta > 0 {
				if !m.TryReserve(delta) {
					// still over — we already reserved sizeHint, keep file but warn
				}
			} else {
				m.usedBytes.Add(delta)
			}
		}
	}
	return n, nil
}
