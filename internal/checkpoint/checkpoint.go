package checkpoint

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// Store persists per-channel cursor for resume.
// File: <output>/.checkpoint.json  { "guildID/channelID": {"last_id":"...", "count":123, "updated":"..."} }
type Store struct {
	path string
	mu   sync.Mutex
	data map[string]Entry
}

type Entry struct {
	LastID  string    `json:"last_id"`
	Count   int64     `json:"count"`
	Updated time.Time `json:"updated"`
}

func New(path string) *Store {
	s := &Store{path: path, data: make(map[string]Entry)}
	// Try load existing
	if b, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(b, &s.data)
	}
	return s
}

func (s *Store) Key(guildID, channelID string) string {
	if guildID != "" {
		return guildID + "/" + channelID
	}
	return "dm/" + channelID
}

func (s *Store) Get(guildID, channelID string) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[s.Key(guildID, channelID)]
	return e, ok
}

func (s *Store) Set(guildID, channelID, lastID string, count int64) error {
	s.mu.Lock()
	s.data[s.Key(guildID, channelID)] = Entry{LastID: lastID, Count: count, Updated: time.Now().UTC()}
	data := s.data
	s.mu.Unlock()
	return s.flush(data)
}

func (s *Store) flush(data map[string]Entry) error {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *Store) All() map[string]Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Entry, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
