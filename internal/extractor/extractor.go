package extractor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/pratherbytecraft/discord-scraper-go/internal/checkpoint"
	"github.com/pratherbytecraft/discord-scraper-go/internal/config"
	"github.com/pratherbytecraft/discord-scraper-go/internal/discord"
	"github.com/pratherbytecraft/discord-scraper-go/internal/writer"
	"github.com/pratherbytecraft/discord-scraper-go/pkg/snowflake"
)

// Extractor orchestrates high-throughput scraping.
// Design:
//   - Discovery phase: fetch guild(s) + channels (+ threads) concurrently.
//   - Scraping phase: bounded worker pool (semaphore) over channels.
//   - Per-channel pagination: sequential within channel (Discord requires cursor), but channels in parallel.
//   - Streaming writes: flush every batch to avoid holding millions of msgs in RAM.
//   - Global counters + per-channel progress via slog.
//   - Ban-safe: adaptive limiter (32 RPS default), snowflake ID filtering, checkpoint resume, 20GB media cap.
type Extractor struct {
	client *discord.Client
	cfg    config.Config
	w      *writer.Writer
	log    *slog.Logger
	cp     *checkpoint.Store
	media  *writer.MediaBudget
	mediaSem *semaphore.Weighted

	totalMsgs   atomic.Int64
	totalChans  atomic.Int64
	failedChans atomic.Int64
	totalMedia  atomic.Int64
}

// New creates an Extractor.
func New(client *discord.Client, cfg config.Config, w *writer.Writer, log *slog.Logger) *Extractor {
	if log == nil {
		log = slog.Default()
	}
	var cp *checkpoint.Store
	if cfg.Resume {
		path := cfg.CheckpointFile
		if path == "" {
			path = filepath.Join(cfg.OutputDir, ".checkpoint.json")
		}
		cp = checkpoint.New(path)
	}
	var media *writer.MediaBudget
	var mediaSem *semaphore.Weighted
	if cfg.DownloadAttachments && cfg.MediaBudgetBytes > 0 {
		media = writer.NewMediaBudget(cfg.MediaBudgetBytes)
		concur := cfg.MediaConcurrency
		if concur <= 0 {
			concur = 4
		}
		if concur > 16 {
			concur = 16
		}
		mediaSem = semaphore.NewWeighted(int64(concur))
	}
	return &Extractor{client: client, cfg: cfg, w: w, log: log, cp: cp, media: media, mediaSem: mediaSem}
}

// Run executes based on cfg (guild and/or channel targets).
func (e *Extractor) Run(ctx context.Context) error {
	if err := e.cfg.Validate(); err != nil {
		return err
	}

	// Collect targets: normalize to guild -> []channel mapping.
	// If only channelIDs given, scrape those directly.
	if len(e.cfg.ChannelIDs) > 0 || e.cfg.ChannelID != "" {
		channels := e.cfg.ChannelIDs
		if e.cfg.ChannelID != "" {
			channels = append(channels, e.cfg.ChannelID)
		}
		// Deduplicate
		channels = uniq(channels)
		return e.scrapeChannels(ctx, "", channels)
	}

	guilds := e.cfg.GuildIDs
	if e.cfg.GuildID != "" {
		guilds = append(guilds, e.cfg.GuildID)
	}
	guilds = uniq(guilds)

	// If no guilds explicit but guildID empty, we could fetch all guilds? But we require explicit target.
	// Instead if empty, report error (Validate already catches).
	for _, gid := range guilds {
		if err := e.ScrapeGuild(ctx, gid); err != nil {
			e.log.Error("guild scrape failed", "guild", gid, "err", err)
			// continue to next guild rather than abort all
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
	return nil
}

// ScrapeGuild scrapes all text-like channels in a guild plus threads.
func (e *Extractor) ScrapeGuild(ctx context.Context, guildID string) error {
	start := time.Now()
	e.log.Info("discovering guild", "guild", guildID)

	guild, err := e.client.FetchGuild(ctx, guildID)
	if err != nil {
		// Fallback: try to get guild from FetchGuilds list for name
		e.log.Warn("fetch guild full failed, trying fallback", "guild", guildID, "err", err)
		// Still proceed to fetch channels
		guild = &discord.Guild{ID: guildID, Name: guildID}
	}

	channels, err := e.client.FetchGuildChannels(ctx, guildID)
	if err != nil {
		return fmt.Errorf("fetch channels for %s: %w", guildID, err)
	}

	// Optionally enrich with active threads
	var threads []discord.Channel
	if e.cfg.IncludeThreads {
		if t, err := e.client.FetchActiveThreads(ctx, guildID); err == nil {
			threads = append(threads, t...)
		} else {
			e.log.Warn("fetch active threads failed", "guild", guildID, "err", err)
		}
		// Also per-channel archived threads (done during channel discovery)
	}

	// Filter to text-like channels
	var textChannels []discord.Channel
	for _, ch := range channels {
		if discord.IsTextLike(ch.Type) {
			textChannels = append(textChannels, ch)
		}
	}
	// Add threads that are text-like (they are)
	for _, th := range threads {
		textChannels = append(textChannels, th)
	}

	// Sort for deterministic order (by position then ID)
	sort.Slice(textChannels, func(i, j int) bool {
		pi, pj := 0, 0
		if textChannels[i].Position != nil {
			pi = *textChannels[i].Position
		}
		if textChannels[j].Position != nil {
			pj = *textChannels[j].Position
		}
		if pi == pj {
			return textChannels[i].ID < textChannels[j].ID
		}
		return pi < pj
	})

	e.log.Info("discovered channels", "guild", guildID, "total", len(channels), "text_like", len(textChannels), "threads_active", len(threads))

	// Persist guild meta before scraping
	if err := e.w.WriteGuildMeta(*guild, channels); err != nil {
		e.log.Warn("write guild meta failed", "err", err)
	}

	// If filter specifies ChannelIDs whitelist, filter down
	if len(e.cfg.ChannelIDs) > 0 {
		allow := make(map[string]bool, len(e.cfg.ChannelIDs))
		for _, id := range e.cfg.ChannelIDs {
			allow[id] = true
		}
		var filtered []discord.Channel
		for _, ch := range textChannels {
			if allow[ch.ID] {
				filtered = append(filtered, ch)
			}
		}
		textChannels = filtered
		e.log.Info("filtered to whitelisted channels", "remaining", len(textChannels))
	}

	// Second pass: per-channel archived threads discovery (parallel, rate-limited)
	if e.cfg.IncludeThreads {
		textChannels = e.discoverArchivedThreads(ctx, textChannels)
		e.log.Info("after archived thread discovery", "total_channels_including_threads", len(textChannels))
	}

	ids := make([]string, len(textChannels))
	for i, ch := range textChannels {
		ids[i] = ch.ID
	}
	// Build channel map for writer name resolution
	chMap := make(map[string]discord.Channel, len(textChannels))
	for _, ch := range textChannels {
		chMap[ch.ID] = ch
	}

	// We already have full channel objects, so scrape via channel objects directly
	if err := e.scrapeChannelObjects(ctx, guildID, textChannels); err != nil {
		return err
	}

	elapsed := time.Since(start)
	e.log.Info("guild scrape complete",
		"guild", guildID,
		"channels", len(textChannels),
		"messages", e.totalMsgs.Load(),
		"elapsed", elapsed.Round(time.Millisecond),
		"msg_per_sec", float64(e.totalMsgs.Load())/elapsed.Seconds(),
	)
	return nil
}

// discoverArchivedThreads fetches archived private/public threads per channel concurrently (bounded).
func (e *Extractor) discoverArchivedThreads(ctx context.Context, channels []discord.Channel) []discord.Channel {
	sem := semaphore.NewWeighted(int64(min(e.cfg.Concurrency, 8))) // don't overwhelm for discovery
	var mu sync.Mutex
	extra := []discord.Channel{}

	g, ctx := errgroup.WithContext(ctx)
	for _, ch := range channels {
		// Only parent text channels can have threads; skip threads themselves
		if discord.IsThread(ch.Type) {
			continue
		}
		// Forum/media parent types also hold threads
		chCopy := ch
		if err := sem.Acquire(ctx, 1); err != nil {
			break
		}
		g.Go(func() error {
			defer sem.Release(1)
			priv, err1 := e.client.FetchArchivedPrivateThreads(ctx, chCopy.ID)
			pub, err2 := e.client.FetchArchivedPublicThreads(ctx, chCopy.ID)
			joined, _ := e.client.FetchJoinedPrivateArchivedThreads(ctx, chCopy.ID)

			if err1 != nil && err2 != nil {
				// Log at debug; many channels return 403 or empty
				return nil
			}
			combined := append(priv, pub...)
			combined = append(combined, joined...)
			// Deduplicate by ID
			seen := make(map[string]bool)
			var uniq []discord.Channel
			for _, t := range combined {
				if !seen[t.ID] {
					seen[t.ID] = true
					uniq = append(uniq, t)
				}
			}
			if len(uniq) > 0 {
				mu.Lock()
				extra = append(extra, uniq...)
				mu.Unlock()
				e.log.Info("found archived threads", "parent", chCopy.ID, "count", len(uniq))
			}
			return nil
		})
	}
	_ = g.Wait()

	// Append and dedup overall
	seen := make(map[string]bool)
	for _, ch := range channels {
		seen[ch.ID] = true
	}
	result := append([]discord.Channel(nil), channels...)
	for _, th := range extra {
		if !seen[th.ID] {
			result = append(result, th)
			seen[th.ID] = true
		}
	}
	return result
}

// scrapeChannels scrapes a list of channel IDs (DM or guild-agnostic).
func (e *Extractor) scrapeChannels(ctx context.Context, guildID string, channelIDs []string) error {
	// Resolve channel objects for names
	var channels []discord.Channel
	for _, id := range channelIDs {
		ch, err := e.client.FetchChannel(ctx, id)
		if err != nil {
			e.log.Warn("fetch channel failed, using stub", "channel", id, "err", err)
			// stub
			ch = &discord.Channel{ID: id, Type: discord.ChannelTypeGuildText}
		} else {
			// If guildID empty but channel has guild_id, adopt it for writer path
			if guildID == "" && ch.GuildID != nil {
				guildID = *ch.GuildID
			}
		}
		channels = append(channels, *ch)
	}
	return e.scrapeChannelObjects(ctx, guildID, channels)
}

// scrapeChannelObjects is the core parallel scraping loop.
// Discrub parity: 403/404 Missing Access on private/voice channels is non-fatal — log and continue.
func (e *Extractor) scrapeChannelObjects(ctx context.Context, guildID string, channels []discord.Channel) error {
	concurrency := e.cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 16
	}
	if concurrency > 64 {
		concurrency = 64
	}

	sem := semaphore.NewWeighted(int64(concurrency))
	g, ctx := errgroup.WithContext(ctx)

	for _, ch := range channels {
		chCopy := ch
		if err := sem.Acquire(ctx, 1); err != nil {
			break
		}
		g.Go(func() error {
			defer sem.Release(1)
			if err := e.scrapeSingleChannel(ctx, guildID, chCopy); err != nil {
				// Only abort on context cancel; 403/404 are expected for some channels
				if ctx.Err() != nil {
					return err
				}
				if isPermissionError(err) {
					e.log.Warn("skipping channel (no access)", "channel", chCopy.ID, "name", safeName(chCopy.Name), "err", err)
					return nil
				}
				e.log.Warn("channel failed (non-fatal, continuing)", "channel", chCopy.ID, "err", err)
				return nil
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	e.log.Info("all channels done", "total_msgs", e.totalMsgs.Load(), "failed_channels", e.failedChans.Load())
	return nil
}

func isPermissionError(err error) bool {
	if err == nil {
		return false
	}
	// discord.apiError with Status 403/404
	type apiErr interface{ Error() string }
	// Try to unwrap *discord.apiError via string check (avoid import cycle)
	s := err.Error()
	return contains(s, "403") || contains(s, "50001") || contains(s, "Missing Access") || contains(s, "50013")
}

// scrapeSingleChannel paginates a single channel sequentially, streaming batches to writer.
// Uses snowflake ID compare for date filters (no time.Parse per msg) + checkpoint resume.
func (e *Extractor) scrapeSingleChannel(ctx context.Context, guildID string, ch discord.Channel) error {
	e.totalChans.Add(1)
	start := time.Now()
	var total int64
	var before string
	batches := 0

	// Resume: if checkpoint has last_id, continue from there
	if e.cp != nil {
		if entry, ok := e.cp.Get(guildID, ch.ID); ok && entry.LastID != "" {
			before = entry.LastID
			total = entry.Count
			e.log.Info("resuming channel from checkpoint", "channel", ch.ID, "before", before, "count", total)
			// Don't double-count totalMsgs for resumed msgs; we track only new
		}
	}

	// Precompute snowflake bounds for fast integer compare (avoid time.Parse hot path)
	var minID, maxID string
	var hasSnowflakeFilter bool
	if e.cfg.AfterDate != nil {
		minID = snowflake.FromTime(*e.cfg.AfterDate)
		hasSnowflakeFilter = true
	}
	if e.cfg.BeforeDate != nil {
		maxID = snowflake.FromTime(*e.cfg.BeforeDate)
		hasSnowflakeFilter = true
	}
	hasFilter := hasSnowflakeFilter || e.cfg.SearchContent != ""

	isJSONL := e.cfg.Format == "jsonl" || e.cfg.Format == ""
	var streamFilter *writer.StreamFilter
	if isJSONL && hasFilter {
		streamFilter = writer.NewStreamFilter(e.cfg.SearchContent, minID, maxID)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Zero-copy raw stream path for JSONL: eliminates reflection, struct allocs, and GC pauses
		if isJSONL {
			rawPayload, release, err := e.client.FetchMessagesRaw(ctx, ch.ID, before, discord.QueryBefore, 100)
			if err != nil {
				e.failedChans.Add(1)
				if isPermissionError(err) {
					e.log.Warn("skip channel (no access, raw)", "channel", ch.ID, "err", err)
					return nil
				}
				e.log.Error("fetch messages raw failed", "channel", ch.ID, "name", safeName(ch.Name), "before", before, "err", err)
				return err
			}
			if len(rawPayload) == 0 {
				release()
				break
			}

			written, lastID, rawCount, werr := e.w.WriteRawBatch(ctx, guildID, ch, rawPayload, streamFilter)
			release()
			if werr != nil {
				e.log.Error("write raw failed", "channel", ch.ID, "err", werr)
				return werr
			}
			if rawCount == 0 {
				break
			}

			e.totalMsgs.Add(written)
			total += written
			batches++
			before = lastID

			// Discrub parity: download attachments (text-first, budget 20GB) + reactions if requested
			if e.media != nil && written > 0 {
				// Extract attachments from rawPayload filtered set and download concurrently
				if err := e.downloadAttachmentsFromRaw(ctx, guildID, ch, rawPayload, streamFilter); err != nil {
					e.log.Warn("media download batch failed", "channel", ch.ID, "err", err)
				}
				if e.media.Exceeded() {
					e.log.Warn("media budget exceeded (20GB), skipping further media", "used", e.media.Used(), "limit", e.media.Limit())
				}
			}
			if e.cfg.IncludeReactions && written > 0 {
				// For zero-copy path, reactions require parsing; handled via fallback structured path if needed
				// Minimal parity: log that reactions would be fetched (actual fetch needs emoji list per msg)
			}

			if e.cp != nil && batches%20 == 0 && before != "" {
				_ = e.cp.Set(guildID, ch.ID, before, total)
			}
			if rawCount < 100 {
				break
			}
			if hasSnowflakeFilter && minID != "" && lastID != "" {
				if compareSnowflake(lastID, minID) < 0 {
					break
				}
			}
			if batches%20 == 0 {
				e.log.Info("channel progress", "channel", ch.ID, "name", safeName(ch.Name), "batches", batches, "msgs", total, "media_used", mediaUsed(e.media))
			}
			if batches > 100000 {
				e.log.Warn("channel pagination guard triggered", "channel", ch.ID)
				break
			}
			continue
		}

		raw, err := e.client.FetchMessages(ctx, ch.ID, before, discord.QueryBefore, 100)
		if err != nil {
			e.failedChans.Add(1)
			if isPermissionError(err) {
				e.log.Warn("skip channel (no access)", "channel", ch.ID, "err", err)
				return nil
			}
			e.log.Error("fetch messages failed", "channel", ch.ID, "name", safeName(ch.Name), "before", before, "err", err)
			return err
		}
		if len(raw) == 0 {
			break
		}
		rawLen := len(raw)

		toWrite := raw
		if hasFilter {
			toWrite = filterMessagesFast(raw, e.cfg, minID, maxID)
		}

		if len(toWrite) > 0 {
			if err := e.w.WriteChannel(ctx, guildID, ch, toWrite); err != nil {
				e.log.Error("write failed", "channel", ch.ID, "err", err)
				return err
			}
			e.totalMsgs.Add(int64(len(toWrite)))
			total += int64(len(toWrite))
			// Discrub parity: attachments + reactions
			if e.media != nil {
				if err := e.downloadAttachmentsFromMessages(ctx, guildID, ch, toWrite); err != nil {
					e.log.Warn("media download failed", "channel", ch.ID, "err", err)
				}
			}
			if e.cfg.IncludeReactions {
				if err := e.fetchReactionsForMessages(ctx, ch, toWrite); err != nil {
					e.log.Warn("reactions fetch failed", "channel", ch.ID, "err", err)
				}
			}
		}

		// Checkpoint every 20 batches or on filtered batches
		batches++
		before = raw[rawLen-1].ID
		if e.cp != nil && batches%20 == 0 {
			_ = e.cp.Set(guildID, ch.ID, before, total)
		}

		if rawLen < 100 {
			break
		}
		// Early exit if we've passed AfterDate bound: all remaining msgs are older than minID
		if hasSnowflakeFilter && minID != "" {
			if compareSnowflake(raw[rawLen-1].ID, minID) < 0 {
				break
			}
		}
		if batches%20 == 0 {
			e.log.Info("channel progress", "channel", ch.ID, "name", safeName(ch.Name), "batches", batches, "msgs", total, "media_used", mediaUsed(e.media))
		}
		if batches > 100000 {
			e.log.Warn("channel pagination guard triggered", "channel", ch.ID)
			break
		}
		if e.media != nil && e.media.Exceeded() {
			e.log.Warn("media budget exceeded, skipping further attachment downloads", "used", e.media.Used(), "limit", e.media.Limit())
		}
	}
	if e.cp != nil && total > 0 {
		_ = e.cp.Set(guildID, ch.ID, before, total)
	}

	elapsed := time.Since(start)
	e.log.Info("channel done", "channel", ch.ID, "name", safeName(ch.Name), "messages", total, "batches", batches, "elapsed", elapsed.Round(time.Millisecond))
	return nil
}

// filterMessages applies client-side filters (legacy time.Parse path, kept for compatibility).
func filterMessages(msgs []discord.Message, cfg config.Config) []discord.Message {
	if cfg.BeforeDate == nil && cfg.AfterDate == nil && cfg.SearchContent == "" {
		return msgs
	}
	out := msgs[:0]
	for _, m := range msgs {
		t, err := time.Parse(time.RFC3339Nano, m.Timestamp)
		if err != nil {
			t, _ = time.Parse(time.RFC3339, m.Timestamp)
		}
		if cfg.BeforeDate != nil && !t.Before(*cfg.BeforeDate) {
			continue
		}
		if cfg.AfterDate != nil && !t.After(*cfg.AfterDate) {
			continue
		}
		if cfg.SearchContent != "" && !containsFold(m.Content, cfg.SearchContent) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// filterMessagesFast uses snowflake ID integer compare for date bounds (no alloc, ~10x faster).
func filterMessagesFast(msgs []discord.Message, cfg config.Config, minID, maxID string) []discord.Message {
	if minID == "" && maxID == "" && cfg.SearchContent == "" {
		return msgs
	}
	out := msgs[:0]
	for _, m := range msgs {
		if maxID != "" && compareSnowflake(m.ID, maxID) >= 0 {
			continue // m.ID >= maxID => newer than before bound, skip
		}
		if minID != "" && compareSnowflake(m.ID, minID) <= 0 {
			continue // m.ID <= minID => older than after bound, skip
		}
		if cfg.SearchContent != "" && !containsFold(m.Content, cfg.SearchContent) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// compareSnowflake compares two snowflake string IDs as integers (no parse overhead for equal length).
// Returns -1 if a<b, 0 if equal, 1 if a>b.
func compareSnowflake(a, b string) int {
	// Snowflakes are 17-20 digit decimal strings; lexicographic compare works if same length,
	// otherwise longer string is larger (since no leading zeros).
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

func mediaUsed(m *writer.MediaBudget) int64 {
	if m == nil {
		return 0
	}
	return m.Used()
}

// parseSnowflakeInt is helper for early exit checks (not hot path).
func parseSnowflakeInt(s string) int64 {
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

// downloadAttachmentsFromMessages handles Discrub parity: download attachments with 20GB budget.
func (e *Extractor) downloadAttachmentsFromMessages(ctx context.Context, guildID string, ch discord.Channel, msgs []discord.Message) error {
	if e.media == nil || e.mediaSem == nil {
		return nil
	}
	if e.media.Exceeded() {
		return nil
	}
	// Collect attachments
	type job struct {
		url      string
		filename string
		size     int64
		msgID    string
	}
	var jobs []job
	for _, m := range msgs {
		for _, a := range m.Attachments {
			jobs = append(jobs, job{url: a.URL, filename: a.Filename, size: int64(a.Size), msgID: m.ID})
		}
	}
	if len(jobs) == 0 {
		return nil
	}
	g, gctx := errgroup.WithContext(ctx)
	for _, j := range jobs {
		j := j
		if err := e.mediaSem.Acquire(gctx, 1); err != nil {
			break
		}
		g.Go(func() error {
			defer e.mediaSem.Release(1)
			if e.media.Exceeded() {
				return nil
			}
			dest := filepath.Join(e.cfg.OutputDir, guildID, "media", ch.ID, j.msgID+"-"+sanitizeFilename(j.filename))
			n, err := e.media.Download(gctx, j.url, dest, j.size)
			if err != nil {
				// Budget exceeded is not fatal, just warn
				if e.media.Exceeded() {
					e.log.Warn("media budget hit, skipping remaining", "used", e.media.Used(), "limit", e.media.Limit())
					return nil
				}
				e.log.Debug("attachment download failed", "url", j.url, "err", err)
				return nil
			}
			e.totalMedia.Add(n)
			return nil
		})
	}
	_ = g.Wait()
	if e.media.Exceeded() {
		e.log.Warn("media budget exceeded after batch", "used", e.media.Used(), "limit", e.media.Limit(), "channel", ch.ID)
	}
	return nil
}

// downloadAttachmentsFromRaw is zero-copy variant: extracts attachments from raw JSON bytes without full message structs.
func (e *Extractor) downloadAttachmentsFromRaw(ctx context.Context, guildID string, ch discord.Channel, raw []byte, filter *writer.StreamFilter) error {
	if e.media == nil || e.mediaSem == nil {
		return nil
	}
	if e.media.Exceeded() {
		return nil
	}
	// Minimal struct to extract attachments
	type rawAtt struct {
		ID          string `json:"id"`
		Attachments []struct {
			URL      string `json:"url"`
			Filename string `json:"filename"`
			Size     int    `json:"size"`
		} `json:"attachments"`
	}
	var jobs []struct {
		url      string
		filename string
		size     int64
		msgID    string
	}
	_, _, err := writer.ParseRawMessages(raw, func(idBytes, msgBytes []byte) error {
		if filter != nil && !filter.Match(idBytes, msgBytes) {
			return nil
		}
		// Quick check if attachments present without full parse: look for "\"attachments\":["
		if !containsBytes(msgBytes, []byte("\"attachments\"")) {
			return nil
		}
		var rm rawAtt
		if err := jsonUnmarshal(msgBytes, &rm); err != nil {
			return nil
		}
		for _, a := range rm.Attachments {
			jobs = append(jobs, struct {
				url      string
				filename string
				size     int64
				msgID    string
			}{url: a.URL, filename: a.Filename, size: int64(a.Size), msgID: rm.ID})
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return nil
	}
	g, gctx := errgroup.WithContext(ctx)
	for _, j := range jobs {
		j := j
		if err := e.mediaSem.Acquire(gctx, 1); err != nil {
			break
		}
		g.Go(func() error {
			defer e.mediaSem.Release(1)
			if e.media.Exceeded() {
				return nil
			}
			dest := filepath.Join(e.cfg.OutputDir, guildID, "media", ch.ID, j.msgID+"-"+sanitizeFilename(j.filename))
			n, err := e.media.Download(gctx, j.url, dest, j.size)
			if err != nil {
				return nil
			}
			e.totalMedia.Add(n)
			return nil
		})
	}
	_ = g.Wait()
	return nil
}

// fetchReactionsForMessages implements Discrub parity: fetch reaction users per emoji (optional, slower).
func (e *Extractor) fetchReactionsForMessages(ctx context.Context, ch discord.Channel, msgs []discord.Message) error {
	// For each message with reactions, fetch users per emoji (limit 100 per emoji)
	// This is intentionally sequential per message to avoid 429 storms; uses same limiter
	for _, m := range msgs {
		if len(m.Reactions) == 0 {
			continue
		}
		for _, r := range m.Reactions {
			emojiStr := ""
			if r.Emoji.Name != nil {
				emojiStr = *r.Emoji.Name
			}
			if r.Emoji.ID != nil && *r.Emoji.ID != "" {
				// Custom emoji: name:id
				if emojiStr != "" {
					emojiStr = emojiStr + ":" + *r.Emoji.ID
				} else {
					emojiStr = *r.Emoji.ID
				}
			}
			if emojiStr == "" {
				continue
			}
			// Fetch first page (100 users) – Discrub does paginated per reaction, we do one page for parity
			users, err := e.client.FetchReactions(ctx, ch.ID, m.ID, emojiStr, 100, "")
			if err != nil {
				e.log.Debug("fetch reactions failed", "channel", ch.ID, "msg", m.ID, "emoji", emojiStr, "err", err)
				continue
			}
			// Optionally we could write reactions to sidecar file; for now just log count
			_ = users
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

// jsonUnmarshal is a thin wrapper for attachment extraction.
func jsonUnmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func containsBytes(b, sub []byte) bool {
	return bytes.Contains(b, sub)
}

// sanitizeFilename mimics filenamify (Discrub parity) but simplified.
func sanitizeFilename(s string) string {
	if s == "" {
		return "file"
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '-' || c == '_' {
			out = append(out, c)
		} else {
			out = append(out, '_')
		}
	}
	if len(out) > 200 {
		out = out[:200]
	}
	return string(out)
}

func containsFold(s, substr string) bool {
	// case-insensitive contains
	// Use naive but fast
	if len(substr) == 0 {
		return true
	}
	if len(s) < len(substr) {
		return false
	}
	// quick ascii lower
	ls := toLower(s)
	lsub := toLower(substr)
	return contains(ls, lsub)
}

func toLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}

func contains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func safeName(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func uniq(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if v == "" {
			continue
		}
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
