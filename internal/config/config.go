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

	// Performance tuning
	RequestsPerSecond int // 0 = auto (rate-limit driven)
	Resume            bool
	Verbose           bool
}

// Default returns sane defaults.
func Default() Config {
	return Config{
		OutputDir:    "./output",
		Format:       "jsonl",
		Concurrency:  16,
		IncludeThreads: true,
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
