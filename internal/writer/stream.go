package writer

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pratherbytecraft/discord-scraper-go/internal/discord"
)

// StreamFilter implements Sparser ("Filter Before You Parse", PVLDB 2018 / arXiv:1803.04509)
// directly on raw byte slices before any JSON deserialization or struct allocation occurs.
type StreamFilter struct {
	ContentLower []byte // lowercase content substring
	MinIDBytes   []byte // snowflake lower bound (messages must be > MinID)
	MaxIDBytes   []byte // snowflake upper bound (messages must be < MaxID)
	MinID        string
	MaxID        string
}

// NewStreamFilter creates a filter from search query and snowflake boundaries.
func NewStreamFilter(content, minID, maxID string) *StreamFilter {
	if content == "" && minID == "" && maxID == "" {
		return nil
	}
	var cLower []byte
	if content != "" {
		cLower = bytes.ToLower([]byte(content))
	}
	var minBytes, maxBytes []byte
	if minID != "" {
		minBytes = []byte(minID)
	}
	if maxID != "" {
		maxBytes = []byte(maxID)
	}
	return &StreamFilter{
		ContentLower: cLower,
		MinIDBytes:   minBytes,
		MaxIDBytes:   maxBytes,
		MinID:        minID,
		MaxID:        maxID,
	}
}

// Match evaluates whether a raw JSON message matches the filter predicates.
// Zero heap allocations.
func (f *StreamFilter) Match(msgIDBytes, msgBytes []byte) bool {
	if f == nil {
		return true
	}
	// 1. MaxID check (older than before bound: ID < MaxID)
	if len(f.MaxIDBytes) > 0 && compareSnowflakeBytes(msgIDBytes, f.MaxIDBytes) >= 0 {
		return false
	}
	// 2. MinID check (newer than after bound: ID > MinID)
	if len(f.MinIDBytes) > 0 && compareSnowflakeBytes(msgIDBytes, f.MinIDBytes) <= 0 {
		return false
	}
	// 3. Substring content check (Sparser AVX2/SSE-accelerated in Go bytes.Contains)
	if len(f.ContentLower) > 0 {
		if !containsFoldBytes(msgBytes, f.ContentLower) {
			return false
		}
	}
	return true
}

// ParseRawMessages scans a raw Discord JSON array [ {...}, {...} ] without unmarshaling.
// Calls yield for each complete JSON object slice.
// Returns:
//   - rawCount: total number of message objects found in the payload
//   - lastID: the snowflake ID string of the last message in the array (for cursor pagination)
//   - err: any structural syntax error encountered
func ParseRawMessages(raw []byte, yield func(idBytes, msgBytes []byte) error) (rawCount int, lastID string, err error) {
	n := len(raw)
	if n == 0 {
		return 0, "", nil
	}

	depth := 0
	inString := false
	escaped := false
	objStart := -1
	var lastIDBytes []byte

	for i := 0; i < n; i++ {
		c := raw[i]

		if escaped {
			escaped = false
			continue
		}
		if c == '\\' && inString {
			escaped = true
			continue
		}
		if c == '"' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}

		if c == '{' {
			if depth == 0 {
				objStart = i
			}
			depth++
		} else if c == '}' {
			depth--
			if depth == 0 && objStart >= 0 {
				msgBytes := raw[objStart : i+1]
				idBytes := ExtractSnowflakeIDBytes(msgBytes)
				rawCount++
				lastIDBytes = idBytes

				if yield != nil {
					if err := yield(idBytes, msgBytes); err != nil {
						if len(lastIDBytes) > 0 {
							lastID = string(lastIDBytes)
						}
						return rawCount, lastID, err
					}
				}
				objStart = -1
			}
		}
	}

	if len(lastIDBytes) > 0 {
		lastID = string(lastIDBytes)
	}
	return rawCount, lastID, nil
}

// ExtractSnowflakeID extracts the top-level "id" snowflake from a raw message JSON object.
func ExtractSnowflakeID(msg []byte) string {
	b := ExtractSnowflakeIDBytes(msg)
	if len(b) == 0 {
		return ""
	}
	return string(b)
}

// ExtractSnowflakeIDBytes returns a sub-slice of the "id" snowflake from a raw message.
// Zero allocations.
func ExtractSnowflakeIDBytes(msg []byte) []byte {
	n := len(msg)
	depth := 0
	inString := false
	escaped := false

	for i := 0; i < n; i++ {
		c := msg[i]
		if escaped {
			escaped = false
			continue
		}
		if c == '\\' && inString {
			escaped = true
			continue
		}
		if c == '"' {
			if !inString {
				if depth == 1 && i+3 < n && msg[i+1] == 'i' && msg[i+2] == 'd' && msg[i+3] == '"' {
					j := i + 4
					for j < n && (msg[j] == ' ' || msg[j] == '\t' || msg[j] == '\r' || msg[j] == '\n') {
						j++
					}
					if j < n && msg[j] == ':' {
						j++
						for j < n && (msg[j] == ' ' || msg[j] == '\t' || msg[j] == '\r' || msg[j] == '\n') {
							j++
						}
						if j < n && msg[j] == '"' {
							startID := j + 1
							endID := startID
							for endID < n && msg[endID] != '"' {
								endID++
							}
							if endID < n {
								return msg[startID:endID]
							}
						}
					}
				}
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}

		if c == '{' {
			depth++
		} else if c == '}' {
			depth--
			if depth <= 0 {
				break
			}
		}
	}
	return nil
}

// WriteRawBatch writes a raw JSON response directly to the channel's NDJSON file.
// Eliminates reflection, deserialization, and re-encoding.
// Specifically tailored for low-memory architectures (e.g. ThinkPad T440p) by avoiding GC churn.
func (w *Writer) WriteRawBatch(ctx context.Context, guildID string, ch discord.Channel, raw []byte, filter *StreamFilter) (written int64, lastID string, rawCount int, err error) {
	if len(raw) == 0 {
		return 0, "", 0, nil
	}

	name := ""
	if ch.Name != nil {
		name = *ch.Name
	}
	cf, err := w.ensureFile(guildID, ch.ID, name)
	if err != nil {
		return 0, "", 0, err
	}

	cf.mu.Lock()
	defer cf.mu.Unlock()

	select {
	case <-ctx.Done():
		return 0, "", 0, ctx.Err()
	default:
	}

	rawCount, lastID, err = ParseRawMessages(raw, func(idBytes, msgBytes []byte) error {
		if filter != nil && !filter.Match(idBytes, msgBytes) {
			return nil
		}
		if _, werr := cf.bw.Write(msgBytes); werr != nil {
			return werr
		}
		if werr := cf.bw.WriteByte('\n'); werr != nil {
			return werr
		}
		written++
		cf.count++
		return nil
	})
	if err != nil {
		return written, lastID, rawCount, fmt.Errorf("write raw batch: %w", err)
	}

	if ferr := cf.bw.Flush(); ferr != nil {
		return written, lastID, rawCount, ferr
	}

	return written, lastID, rawCount, nil
}

// containsFoldBytes checks if b contains sub (lowercase) case-insensitively.
func containsFoldBytes(b, subLower []byte) bool {
	if len(subLower) == 0 {
		return true
	}
	if len(b) < len(subLower) {
		return false
	}
	// Quick scan using ASCII lower
	subLen := len(subLower)
	maxI := len(b) - subLen
	for i := 0; i <= maxI; i++ {
		match := true
		for j := 0; j < subLen; j++ {
			c := b[i+j]
			if c >= 'A' && c <= 'Z' {
				c += 32
			}
			if c != subLower[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// compareSnowflakeBytes compares two snowflake decimal byte slices as numbers.
// Avoids converting to strings or parsing to int64. Zero allocations.
func compareSnowflakeBytes(a, b []byte) int {
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	return bytes.Compare(a, b)
}

// compareSnowflake compares two snowflake string IDs as numeric values.
func compareSnowflake(a, b string) int {
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
