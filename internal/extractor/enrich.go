package extractor

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/pratherbytecraft/discord-scraper-go/internal/discord"
)

// EnrichUsers collects all unique author IDs from JSONL and fetches full User objects.
// Discrub parity: fetchGuildUser / getUser per author. Writes output/<guild>/users.json
// Note: /guilds/{id}/members is 403 for user tokens (Missing Access), so we can only get
// profiles that have posted messages (visible profiles). This is the maximal set we can see.
func (e *Extractor) EnrichUsers(ctx context.Context, guildID string) error {
	if e.cfg.IncludeUsers == false {
		// Still allow if user explicitly wants all profiles; default true for analysis
		return nil
	}
	pattern := filepath.Join(e.cfg.OutputDir, guildID, "*.jsonl")
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) == 0 {
		// Also try guildID/ subdir
		matches, _ = filepath.Glob(filepath.Join(e.cfg.OutputDir, guildID, "*.jsonl"))
		if len(matches) == 0 {
			matches, _ = filepath.Glob(filepath.Join(e.cfg.OutputDir, "**", "*.jsonl"))
		}
	}
	// Collect unique author IDs by scanning JSONL (streaming, low memory)
	unique := make(map[string]bool)
	for _, f := range matches {
		fh, err := os.Open(f)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(fh)
		// Increase buffer for large messages
		buf := make([]byte, 64*1024)
		scanner.Buffer(buf, 10*1024*1024)
		for scanner.Scan() {
			var m discord.Message
			if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
				continue
			}
			if m.Author.ID != "" {
				unique[m.Author.ID] = true
			}
			for _, u := range m.Mentions {
				if u.ID != "" {
					unique[u.ID] = true
				}
			}
		}
		fh.Close()
	}
	if len(unique) == 0 {
		e.log.Info("no users to enrich")
		return nil
	}
	e.log.Info("enriching users", "guild", guildID, "unique", len(unique))

	// Fetch concurrently with limiter (reuse client's limiter)
	sem := semaphore.NewWeighted(8) // 8 concurrent fetches, limiter will pace to 28 RPS
	var mu sync.Mutex
	users := make(map[string]*discord.User, len(unique))
	g, ctx := errgroup.WithContext(ctx)
	for uid := range unique {
		uid := uid
		if err := sem.Acquire(ctx, 1); err != nil {
			break
		}
		g.Go(func() error {
			defer sem.Release(1)
			u, err := e.client.FetchUser(ctx, uid)
			if err != nil {
				e.log.Debug("fetch user failed", "id", uid, "err", err)
				return nil
			}
			mu.Lock()
			users[uid] = u
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()

	// Also try to get self + guild preview
	guildPreview, _ := e.client.FetchGuild(ctx, guildID)

	// Write users.json
	outPath := filepath.Join(e.cfg.OutputDir, guildID, "users.json")
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	payload := map[string]any{
		"guild_id": guildID,
		"guild":    guildPreview,
		"count":    len(users),
		"users":    users,
	}
	if err := enc.Encode(payload); err != nil {
		return err
	}
	e.log.Info("users enriched", "guild", guildID, "count", len(users), "path", outPath)
	return nil
}
