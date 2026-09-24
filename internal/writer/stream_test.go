package writer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pratherbytecraft/discord-scraper-go/internal/discord"
)

func TestParseRawMessages(t *testing.T) {
	raw := []byte(`[
		{"id": "111", "content": "first message"},
		{"id": "222", "content": "second message with } bracket"},
		{"id": "333", "content": "third message with \"escaped\" quotes"}
	]`)

	var ids []string
	count, lastID, err := ParseRawMessages(raw, func(idBytes, msgBytes []byte) error {
		ids = append(ids, string(idBytes))
		return nil
	})
	if err != nil {
		t.Fatalf("ParseRawMessages failed: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected count 3, got %d", count)
	}
	if lastID != "333" {
		t.Fatalf("expected lastID 333, got %q", lastID)
	}
	if len(ids) != 3 || ids[0] != "111" || ids[1] != "222" || ids[2] != "333" {
		t.Fatalf("unexpected ids: %v", ids)
	}
}

func TestExtractSnowflakeID(t *testing.T) {
	cases := []struct {
		json string
		want string
	}{
		{`{"id":"1234567890","content":"hello"}`, "1234567890"},
		{`{ "id" : "9876543210" , "author": {"id": "111"} }`, "9876543210"},
		{`{"type": 0, "id":"5555555555"}`, "5555555555"},
		{`{"content": "message with id in it", "id": "4444444444"}`, "4444444444"},
	}

	for _, tc := range cases {
		got := ExtractSnowflakeID([]byte(tc.json))
		if got != tc.want {
			t.Errorf("ExtractSnowflakeID(%s) = %q, want %q", tc.json, got, tc.want)
		}
	}
}

func TestStreamFilter(t *testing.T) {
	filter := NewStreamFilter("special", "100", "500")

	// Matches: content has "special", ID between 100 and 500
	msg1 := []byte(`{"id":"200","content":"this is a Special message"}`)
	if !filter.Match([]byte("200"), msg1) {
		t.Errorf("expected msg1 to match filter")
	}

	// Fails: no "special"
	msg2 := []byte(`{"id":"200","content":"ordinary message"}`)
	if filter.Match([]byte("200"), msg2) {
		t.Errorf("expected msg2 to NOT match filter")
	}

	// Fails: ID <= 100
	msg3 := []byte(`{"id":"099","content":"special message"}`)
	if filter.Match([]byte("099"), msg3) {
		t.Errorf("expected msg3 to NOT match filter (too old)")
	}

	// Fails: ID >= 500
	msg4 := []byte(`{"id":"600","content":"special message"}`)
	if filter.Match([]byte("600"), msg4) {
		t.Errorf("expected msg4 to NOT match filter (too new)")
	}
}

func TestWriteRawBatch(t *testing.T) {
	dir := t.TempDir()
	w, err := New(dir, "jsonl")
	if err != nil {
		t.Fatalf("New writer: %v", err)
	}
	defer w.Close()

	ch := discord.Channel{ID: "chan100"}
	name := "benchmark"
	ch.Name = &name

	rawPayload := []byte(`[
		{"id": "1001", "content": "alpha"},
		{"id": "1002", "content": "beta"},
		{"id": "1003", "content": "gamma"}
	]`)

	written, lastID, rawCount, err := w.WriteRawBatch(context.Background(), "guild1", ch, rawPayload, nil)
	if err != nil {
		t.Fatalf("WriteRawBatch: %v", err)
	}
	if written != 3 || rawCount != 3 {
		t.Fatalf("expected written=3, rawCount=3, got written=%d, rawCount=%d", written, rawCount)
	}
	if lastID != "1003" {
		t.Fatalf("expected lastID 1003, got %q", lastID)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	// Verify file content
	matches, _ := filepath.Glob(filepath.Join(dir, "guild1", "*.jsonl"))
	if len(matches) != 1 {
		t.Fatalf("expected 1 file, got %v", matches)
	}
	data, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d: %q", len(lines), string(data))
	}
	if !strings.Contains(lines[0], `"alpha"`) || !strings.Contains(lines[2], `"gamma"`) {
		t.Fatalf("unexpected line contents: %v", lines)
	}
}
