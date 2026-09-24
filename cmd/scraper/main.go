package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/pratherbytecraft/discord-scraper-go/internal/config"
	"github.com/pratherbytecraft/discord-scraper-go/internal/discord"
	"github.com/pratherbytecraft/discord-scraper-go/internal/extractor"
	"github.com/pratherbytecraft/discord-scraper-go/internal/writer"
)

var (
	cfg config.Config
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	cfg = config.Default()

	var (
		tokenFlag      string
		guildFlag      string
		guildsFlag     string
		channelFlag    string
		channelsFlag   string
		outputFlag     string
		formatFlag     string
		concurrencyFlg int
		beforeFlag     string
		afterFlag      string
		contentFlag    string
		verboseFlag    bool
		includeThreadsFlag bool
		dryRunFlag     bool
	)

	cmd := &cobra.Command{
		Use:   "scraper",
		Short: "Blazing-fast Discord scraper — maximally extracts all messages from a server",
		Long: `DiscordScraperGo is a high-performance Discord data extractor inspired by Discrub.

Features:
  • Concurrent channel scraping with bounded worker pools (goroutines + semaphore)
  • Token-bucket rate limiter that respects Discord's X-RateLimit headers + 429 retry_after
  • Streaming NDJSON/CSV output (no RAM blowup on huge servers)
  • Guild, channel, thread, role, and message extraction
  • Resume-safe pagination (cursor = before snowflake)
  • Filtering by date/content/author (client-side fast path, server-side search optional)

Examples:
  # Scrape whole guild
  scraper --token $DISCORD_TOKEN --guild 123456789012345678 --output ./output --concurrency 24

  # Scrape single channel
  scraper --token $TOKEN --channel 987654321098765432

  # With date filter and CSV export
  scraper --token $TOKEN --guild 123... --after 2024-01-01 --before 2024-06-01 --format csv

Environment:
  DISCORD_TOKEN is read if --token is not provided.
`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Resolve token
			tok := tokenFlag
			if tok == "" {
				tok = os.Getenv("DISCORD_TOKEN")
			}
			if tok == "" {
				// also check TOKEN
				tok = os.Getenv("TOKEN")
			}
			// Allow "Bot " prefix or raw
			tok = strings.TrimSpace(tok)
			if tok == "" {
				return fmt.Errorf("token required: pass --token or set DISCORD_TOKEN")
			}
			cfg.Token = tok
			cfg.OutputDir = outputFlag
			cfg.Format = strings.ToLower(formatFlag)
			cfg.Concurrency = concurrencyFlg
			cfg.SearchContent = contentFlag
			cfg.Verbose = verboseFlag
			cfg.IncludeThreads = includeThreadsFlag

			if guildFlag != "" {
				cfg.GuildID = strings.TrimSpace(guildFlag)
			}
			if guildsFlag != "" {
				for _, g := range strings.Split(guildsFlag, ",") {
					if s := strings.TrimSpace(g); s != "" {
						cfg.GuildIDs = append(cfg.GuildIDs, s)
					}
				}
			}
			if channelFlag != "" {
				cfg.ChannelID = strings.TrimSpace(channelFlag)
			}
			if channelsFlag != "" {
				for _, c := range strings.Split(channelsFlag, ",") {
					if s := strings.TrimSpace(c); s != "" {
						cfg.ChannelIDs = append(cfg.ChannelIDs, s)
					}
				}
			}
			if beforeFlag != "" {
				t, err := parseDate(beforeFlag)
				if err != nil {
					return fmt.Errorf("invalid --before %q: %w", beforeFlag, err)
				}
				cfg.BeforeDate = &t
			}
			if afterFlag != "" {
				t, err := parseDate(afterFlag)
				if err != nil {
					return fmt.Errorf("invalid --after %q: %w", afterFlag, err)
				}
				cfg.AfterDate = &t
			}
			if dryRunFlag {
				// handled in RunE
			}
			return cfg.Validate()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			logLevel := slog.LevelInfo
			if verboseFlag {
				logLevel = slog.LevelDebug
			}
			handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})
			log := slog.New(handler)

			if dryRunFlag {
				log.Info("dry-run: config valid", "config", fmt.Sprintf("%+v", cfg))
				return nil
			}

			// Context with cancel on SIGINT/SIGTERM
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			log.Info("starting scraper",
				"output", cfg.OutputDir,
				"format", cfg.Format,
				"concurrency", cfg.Concurrency,
				"guild", cfg.GuildID,
				"guilds", strings.Join(cfg.GuildIDs, ","),
				"channel", cfg.ChannelID,
				"channels", strings.Join(cfg.ChannelIDs, ","),
				"include_threads", cfg.IncludeThreads,
			)

			client := discord.NewClient(cfg.Token)

			// Quick auth check
			log.Info("verifying token")
			self, err := client.FetchSelf(ctx)
			if err != nil {
				return fmt.Errorf("token verification failed (/users/@me): %w", err)
			}
			log.Info("authenticated", "user", self.Username, "id", self.ID)

			w, err := writer.New(cfg.OutputDir, cfg.Format)
			if err != nil {
				return fmt.Errorf("create writer: %w", err)
			}
			defer func() {
				if err := w.Close(); err != nil {
					log.Error("writer close failed", "err", err)
				}
			}()

			ext := extractor.New(client, cfg, w, log)
			start := time.Now()
			if err := ext.Run(ctx); err != nil {
				return fmt.Errorf("scrape failed: %w", err)
			}
			elapsed := time.Since(start)
			stats := w.Stats()
			var totalMsgs int64
			for _, c := range stats {
				totalMsgs += c
			}
			log.Info("scrape complete",
				"elapsed", elapsed.Round(time.Millisecond),
				"total_messages", totalMsgs,
				"output_dir", cfg.OutputDir,
			)
			// Print summary to stdout as JSON-ish for scripting
			fmt.Fprintf(os.Stdout, "\nDone. %d messages -> %s (elapsed %s)\n", totalMsgs, cfg.OutputDir, elapsed.Round(time.Second))
			return nil
		},
	}

	cmd.Flags().StringVar(&tokenFlag, "token", "", "Discord token (or set DISCORD_TOKEN env). Prefix with 'Bot ' for bot tokens, raw for user tokens")
	cmd.Flags().StringVar(&guildFlag, "guild", "", "Guild (server) ID to scrape (all text channels)")
	cmd.Flags().StringVar(&guildsFlag, "guilds", "", "Comma-separated guild IDs (alternative to --guild)")
	cmd.Flags().StringVar(&channelFlag, "channel", "", "Single channel ID to scrape")
	cmd.Flags().StringVar(&channelsFlag, "channels", "", "Comma-separated channel IDs")
	cmd.Flags().StringVar(&outputFlag, "output", "./output", "Output directory")
	cmd.Flags().StringVar(&formatFlag, "format", "jsonl", "Output format: jsonl (default), json, csv")
	cmd.Flags().IntVar(&concurrencyFlg, "concurrency", 16, "Max concurrent channel workers (1-64). Higher = faster, but more rate-limit hits")
	cmd.Flags().StringVar(&beforeFlag, "before", "", "Only messages before this date (YYYY-MM-DD or RFC3339)")
	cmd.Flags().StringVar(&afterFlag, "after", "", "Only messages after this date (YYYY-MM-DD or RFC3339)")
	cmd.Flags().StringVar(&contentFlag, "content", "", "Only messages containing this substring (case-insensitive, client-side filter)")
	cmd.Flags().BoolVar(&verboseFlag, "verbose", false, "Verbose debug logging")
	cmd.Flags().BoolVar(&includeThreadsFlag, "include-threads", true, "Include archived and active threads")
	cmd.Flags().BoolVar(&dryRunFlag, "dry-run", false, "Validate args and exit without scraping")

	// Guild/channel inspection subcommand
	cmd.AddCommand(inspectCmd())
	cmd.AddCommand(versionCmd())

	return cmd
}

func parseDate(s string) (time.Time, error) {
	// Try RFC3339, then date-only, then unix timestamp
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unsupported date format %q (try YYYY-MM-DD or RFC3339)", s)
}

func inspectCmd() *cobra.Command {
	var tokenFlag, guildFlag, channelFlag string
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect guilds/channels without scraping messages",
		RunE: func(cmd *cobra.Command, args []string) error {
			tok := tokenFlag
			if tok == "" {
				tok = os.Getenv("DISCORD_TOKEN")
			}
			if tok == "" {
				return fmt.Errorf("token required")
			}
			ctx := context.Background()
			client := discord.NewClient(strings.TrimSpace(tok))
			self, err := client.FetchSelf(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("Authenticated as %s#%s (%s)\n", self.Username, self.Discriminator, self.ID)

			if channelFlag != "" {
				ch, err := client.FetchChannel(ctx, channelFlag)
				if err != nil {
					return err
				}
				fmt.Printf("Channel %s (%s) type=%d guild=%v\n", ch.ID, safeStr(ch.Name), ch.Type, safeStr(ch.GuildID))
				return nil
			}
			if guildFlag != "" {
				channels, err := client.FetchGuildChannels(ctx, guildFlag)
				if err != nil {
					return err
				}
				fmt.Printf("Guild %s has %d channels:\n", guildFlag, len(channels))
				for _, ch := range channels {
					fmt.Printf("  %-22s %-25s type=%-2d parent=%s\n", ch.ID, safeStr(ch.Name), ch.Type, safeStr(ch.ParentID))
				}
				return nil
			}
			guilds, err := client.FetchGuilds(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("User is in %d guilds:\n", len(guilds))
			for _, g := range guilds {
				fmt.Printf("  %s  %s\n", g.ID, g.Name)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&tokenFlag, "token", "", "Discord token")
	cmd.Flags().StringVar(&guildFlag, "guild", "", "Guild ID to list channels")
	cmd.Flags().StringVar(&channelFlag, "channel", "", "Channel ID to inspect")
	return cmd
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("discord-scraper-go v0.1.0 (go1.27, cobra, errgroup, streaming writer)")
		},
	}
}

func safeStr(s *string) string {
	if s == nil {
		return "-"
	}
	return *s
}
