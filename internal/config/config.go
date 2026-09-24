package config

import "time"

// Config holds all extractor settings.
type Config struct {
	Token               string
	GuildID             string
	ChannelID           string
	GuildIDs            []string
	ChannelIDs          []string
	OutputDir           string
	Format              string // jsonl, json, csv

	Concurrency         int
	IncludeThreads      bool
	IncludeDMs          bool
	DownloadAttachments bool
	MediaDir            string

	// Filtering (optional, mirrors Discrub SearchCriteria)
	SearchContent string
	BeforeDate    *time.Time
	AfterDate     *time.Time
	AuthorIDs     []string
	MentionIDs    []string
	HasTypes      []string
	Pinned        *bool

	// Performance tuning (ban-safe)
	RequestsPerSecond int  // target RPS for global limiter (32 default, 0=auto)
	TargetRPS         int  // alias for RequestsPerSecond
	Resume            bool // resume from checkpoint (skips already-scraped messages)
	Verbose           bool
	CheckpointFile    string // path to resume checkpoint json (default <output>/.checkpoint.json)

	// DuckDB / Parquet
	DuckDB            bool   // also generate DuckDB ingest SQL + optional .duckdb file
	Parquet           bool   // write parquet alongside jsonl (requires parquet-go)

	// Media budget (20GB default when enabled)
	MediaBudgetBytes int64 // 0 = disabled, else cap (e.g. 20*1024*1024*1024)
	MediaConcurrency int   // download workers (default 4)

	// Discrub parity: reactions / users
	IncludeReactions bool // fetch reactions per message (extra API calls, slower)
	IncludeUsers     bool // fetch user lookups (future)
}

// Default returns hardware-optimized defaults (tuned for Intel 4-core/8-thread CPU like ThinkPad T440p).
func Default() Config {
	return Config{
		OutputDir:        "./output",
		Format:           "jsonl",
		Concurrency:      8,  // 1 worker per logical thread on i7-4712MQ
		IncludeThreads:   true,
		TargetRPS:        36, // high throughput, ban-safe headroom under Discord 50
		RequestsPerSecond: 36,
		MediaBudgetBytes: 20 * 1024 * 1024 * 1024,
		MediaConcurrency: 4,
	}
}

// Validate checks config.
func (c Config) Validate() error {
	if c.Token == "" {
		return ErrMissingToken
	}
	if c.GuildID == "" && c.ChannelID == "" && len(c.GuildIDs) == 0 && len(c.ChannelIDs) == 0 {
		return ErrMissingTarget
	}
	if c.Concurrency <= 0 {
		return ErrBadConcurrency
	}
	return nil
}

var (
	ErrMissingToken   = errStr("token is required (use --token or DISCORD_TOKEN env)")
	ErrMissingTarget  = errStr("provide --guild or --channel (or lists)")
	ErrBadConcurrency = errStr("concurrency must be > 0")
)

type errStr string

func (e errStr) Error() string { return string(e) }
