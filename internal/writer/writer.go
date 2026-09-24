package writer

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pratherbytecraft/discord-scraper-go/internal/discord"
)

// Writer streams messages to disk with minimal memory overhead.
// Each channel gets its own file: <outputDir>/<guildID>/<channelID>.jsonl
// Guild metadata goes to <outputDir>/<guildID>/guild.json, channels.json, etc.
// Thread-safe and buffered.
type Writer struct {
	baseDir string
	format  string // jsonl | json | csv
	mu      sync.Mutex
	files   map[string]*channelFile
}

type channelFile struct {
	f       *os.File
	bw      *bufio.Writer
	enc     *json.Encoder // for jsonl
	csvW    *csv.Writer
	count   int64
	header  bool // csv header written?
	mu      sync.Mutex
}

// New creates a Writer.
func New(baseDir, format string) (*Writer, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}
	if format == "" {
		format = "jsonl"
	}
	return &Writer{
		baseDir: baseDir,
		format:  format,
		files:   make(map[string]*channelFile),
	}, nil
}

// key returns file map key.
func (w *Writer) key(guildID, channelID string) string {
	if guildID != "" {
		return guildID + "/" + channelID
	}
	return "dm/" + channelID
}

// ensureFile opens (or reuses) the file for a channel.
func (w *Writer) ensureFile(guildID, channelID, channelName string) (*channelFile, error) {
	k := w.key(guildID, channelID)
	w.mu.Lock()
	if cf, ok := w.files[k]; ok {
		w.mu.Unlock()
		return cf, nil
	}
	w.mu.Unlock()

	dir := filepath.Join(w.baseDir, safe(guildIDOrDM(guildID)))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// Filename: channelID[_name].ext ; sanitize name for readability but keep ID primary.
	ext := "jsonl"
	if w.format == "csv" {
		ext = "csv"
	} else if w.format == "json" {
		ext = "json"
	}
	base := channelID
	if channelName != "" {
		base = fmt.Sprintf("%s-%s", channelID, safe(channelName))
	}
	path := filepath.Join(dir, base+"."+ext)

	// If resume disabled, truncate; else append.
	flag := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	// Count existing lines if appending
	var existing int64
	if _, err := os.Stat(path); err == nil {
		// quick count? we keep count from 0 and will append.
		existing = 0 // we could count but not needed for correctness
		_ = existing
	}
	f, err := os.OpenFile(path, flag, 0o644)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriterSize(f, 256*1024)

	cf := &channelFile{f: f, bw: bw, count: 0}
	if w.format == "jsonl" || w.format == "json" {
		enc := json.NewEncoder(bw)
		enc.SetEscapeHTML(false)
		cf.enc = enc
	}
	if w.format == "csv" {
		cf.csvW = csv.NewWriter(bw)
	}

	w.mu.Lock()
	// double-check race
	if existingCF, ok := w.files[k]; ok {
		// someone opened while we were creating, close ours
		_ = f.Close()
		w.mu.Unlock()
		return existingCF, nil
	}
	w.files[k] = cf
	w.mu.Unlock()
	return cf, nil
}

// WriteChannel writes a batch of messages.
// ctx is checked for cancellation between batches.
func (w *Writer) WriteChannel(ctx context.Context, guildID string, ch discord.Channel, msgs []discord.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	name := ""
	if ch.Name != nil {
		name = *ch.Name
	}
	cf, err := w.ensureFile(guildID, ch.ID, name)
	if err != nil {
		return err
	}
	cf.mu.Lock()
	defer cf.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	switch w.format {
	case "csv":
		if !cf.header {
			if err := cf.csvW.Write([]string{
				"id", "channel_id", "author_id", "author_username", "content",
				"timestamp", "edited_timestamp", "pinned", "type", "attachments", "embeds",
			}); err != nil {
				return err
			}
			cf.header = true
		}
		for _, m := range msgs {
			attJSON, _ := json.Marshal(m.Attachments)
			embJSON, _ := json.Marshal(m.Embeds)
			edited := ""
			if m.EditedTimestamp != nil {
				edited = *m.EditedTimestamp
			}
			rec := []string{
				m.ID, m.ChannelID, m.Author.ID, m.Author.Username,
				m.Content, m.Timestamp, edited,
				fmt.Sprint(m.Pinned), fmt.Sprint(m.Type),
				string(attJSON), string(embJSON),
			}
			if err := cf.csvW.Write(rec); err != nil {
				return err
			}
		}
		cf.csvW.Flush()
		if err := cf.csvW.Error(); err != nil {
			return err
		}
	default: // jsonl
		for _, m := range msgs {
			if err := cf.enc.Encode(m); err != nil {
				return err
			}
		}
	}
	cf.count += int64(len(msgs))
	return cf.bw.Flush()
}

// WriteGuildMeta writes guild.json and channels.json.
func (w *Writer) WriteGuildMeta(guild discord.Guild, channels []discord.Channel) error {
	dir := filepath.Join(w.baseDir, safe(guild.ID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "guild.json"), guild); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "channels.json"), channels); err != nil {
		return err
	}
	return nil
}

// WriteJSON is a generic helper for single JSON dumps.
func (w *Writer) WriteJSON(relPath string, v any) error {
	path := filepath.Join(w.baseDir, relPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return writeJSON(path, v)
}

// Close flushes and closes all files.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var firstErr error
	for _, cf := range w.files {
		cf.mu.Lock()
		_ = cf.bw.Flush()
		if cf.csvW != nil {
			cf.csvW.Flush()
		}
		if err := cf.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		cf.mu.Unlock()
	}
	return firstErr
}

// Stats returns per-channel counts (for summary).
func (w *Writer) Stats() map[string]int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	m := make(map[string]int64, len(w.files))
	for k, cf := range w.files {
		m[k] = cf.count
	}
	return m
}

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriterSize(f, 64*1024)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return err
	}
	return bw.Flush()
}

func safe(s string) string {
	// sanitize for filesystem
	if s == "" {
		return "unknown"
	}
	// allow alnum, -, _
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			out = append(out, c)
		} else {
			out = append(out, '_')
		}
	}
	return string(out)
}

func guildIDOrDM(id string) string {
	if id == "" {
		return "dm"
	}
	return id
}

// Ensure interfaces for jsonl streaming readers
var _ io.Writer = (*bufio.Writer)(nil)

// Discard helper for tests
var _ = context.Background
