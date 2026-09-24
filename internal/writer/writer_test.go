package writer

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/pratherbytecraft/discord-scraper-go/internal/discord"
)

func TestWriterJSONL(t *testing.T) {
	dir := t.TempDir()
	w, err := New(dir, "jsonl")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer w.Close()

	ch := discord.Channel{ID: "123", Type: 0}
	name := "general"
	ch.Name = &name
	msgs := []discord.Message{
		{ID: "1", ChannelID: "123", Content: "hello", Timestamp: "2024-01-01T00:00:00.000Z", Author: discord.User{ID: "u1", Username: "alice"}},
		{ID: "2", ChannelID: "123", Content: "world", Timestamp: "2024-01-01T00:01:00.000Z", Author: discord.User{ID: "u2", Username: "bob"}},
	}
	if err := w.WriteChannel(context.Background(), "999", ch, msgs); err != nil {
		t.Fatalf("WriteChannel: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Verify file exists and contains 2 lines
	matches, _ := filepath.Glob(filepath.Join(dir, "999", "*.jsonl"))
	if len(matches) != 1 {
		t.Fatalf("expected 1 file, got %v", matches)
	}
	f, _ := os.Open(matches[0])
	defer f.Close()
	dec := json.NewDecoder(f)
	count := 0
	for dec.More() || count == 0 {
		var m discord.Message
		if err := dec.Decode(&m); err != nil {
			break
		}
		count++
	}
	// simpler line count
	data, _ := os.ReadFile(matches[0])
	lines := 0
	for _, c := range data {
		if c == '\n' {
			lines++
		}
	}
	if lines != 2 {
		t.Fatalf("expected 2 lines, got %d", lines)
	}
}

func TestWriterCSV(t *testing.T) {
	dir := t.TempDir()
	w, err := New(dir, "csv")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer w.Close()
	ch := discord.Channel{ID: "456", Type: 0}
	name := "test"
	ch.Name = &name
	msgs := []discord.Message{
		{ID: "10", ChannelID: "456", Content: "hi", Timestamp: "2024-01-02T00:00:00Z", Author: discord.User{ID: "u1", Username: "alice"}},
	}
	if err := w.WriteChannel(context.Background(), "999", ch, msgs); err != nil {
		t.Fatalf("WriteChannel csv: %v", err)
	}
	w.Close()
	matches, _ := filepath.Glob(filepath.Join(dir, "999", "*.csv"))
	if len(matches) != 1 {
		t.Fatalf("expected csv file, got %v", matches)
	}
}
