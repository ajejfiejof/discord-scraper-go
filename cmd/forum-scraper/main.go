package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/pratherbytecraft/discord-scraper-go/internal/forum"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var (
	forumURL string
	outputDir string
	cookieFile string
	cookieStr string
	concurrency int
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "forum-scraper",
		Short: "Discourse forum scraper for example.com — Go, realistic headers, checkpoint",
		Long: `ForumScraper — Discourse API scraper for discussion forum.

Uses realistic browser headers + _forum_session cookie from ~/forum.har (or --cookie-file/--cookie).
Reuses the same writer/checkpoint/DuckDB patterns as the Discord scraper.

Examples:
  forum-scraper --forum-url https://example.com --cookie-file ~/forum.har --output ./forum-output --concurrency 8
  forum-scraper --cookie " _forum_session=..." --output ./forum-output --verbose

Docs: see docs/ARCHITECTURE.md (forum client mirrors discord/client.go)
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			logLevel := slog.LevelInfo
			if v, _ := cmd.Flags().GetBool("verbose"); v {
				logLevel = slog.LevelDebug
			}
			log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))

			// Resolve cookie
			cookie := cookieStr
			if cookie == "" && cookieFile != "" {
				// Try HAR first, then raw cookie file
				if strings.HasSuffix(cookieFile, ".har") {
					b, err := os.ReadFile(cookieFile)
					if err != nil {
						return fmt.Errorf("read har: %w", err)
					}
					var har struct {
						Log struct {
							Entries []struct {
								Request struct {
									Headers []struct {
										Name  string `json:"name"`
										Value string `json:"value"`
									} `json:"headers"`
								} `json:"request"`
							} `json:"entries"`
						} `json:"log"`
					}
					if err := json.Unmarshal(b, &har); err != nil {
						return fmt.Errorf("parse har: %w", err)
					}
					for _, h := range har.Log.Entries[0].Request.Headers {
						if strings.ToLower(h.Name) == "cookie" {
							cookie = h.Value
							break
						}
					}
				} else {
					b, err := os.ReadFile(cookieFile)
					if err != nil {
						return fmt.Errorf("read cookie file: %w", err)
					}
					cookie = strings.TrimSpace(string(b))
				}
			}
			if cookie == "" {
				cookie = os.Getenv("DISCOURSE_COOKIE")
				if cookie == "" {
					cookie = os.Getenv("FORUM_COOKIE")
				}
			}
			if cookie == "" {
				return fmt.Errorf("cookie required: --cookie, --cookie-file ~/forum.har, or DISCOURSE_COOKIE env")
			}

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			if err := os.MkdirAll(outputDir, 0755); err != nil {
				return err
			}

			client := forum.NewClient(forumURL, cookie)
			log.Info("starting forum scrape", "forum", forumURL, "output", outputDir, "concurrency", concurrency)

			// Quick auth check: fetch categories
			cats, err := client.FetchCategories(ctx)
			if err != nil {
				return fmt.Errorf("fetch categories (check cookie): %w", err)
			}
			log.Info("authenticated, categories found", "count", len(cats.CategoryList.Categories))
			for _, c := range cats.CategoryList.Categories {
				log.Debug("category", "id", c.ID, "name", c.Name, "slug", c.Slug)
			}
			// Save categories
			if err := writeJSON(filepath.Join(outputDir, "categories.json"), cats); err != nil {
				return err
			}

			// Paginate 100%: latest + top + per-category (until empty) — 100% forum
			allTopics := make(map[int]forum.Topic)
			// Helper to paginate any fetcher until empty
			paginate := func(name string, fetch func(context.Context, int) (*forum.TopicListResponse, error)) {
				for page := 0; ; page++ {
					resp, err := fetch(ctx, page)
					if err != nil {
						log.Warn("fetch page failed", "name", name, "page", page, "err", err)
						break
					}
					if len(resp.TopicList.Topics) == 0 {
						break
					}
					for _, t := range resp.TopicList.Topics {
						allTopics[t.ID] = t
					}
					log.Info("topics page", "source", name, "page", page, "total_unique", len(allTopics))
					if resp.TopicList.MoreTopicsURL == "" && len(resp.TopicList.Topics) < 30 {
						break
					}
					select {
					case <-ctx.Done():
						return
					default:
					}
					if page > 200 {
						log.Warn("paginate guard", "source", name, "page", page)
						break
					}
				}
			}
			paginate("latest", func(ctx context.Context, page int) (*forum.TopicListResponse, error) { return client.FetchLatest(ctx, page) })
			paginate("top_weekly", func(ctx context.Context, page int) (*forum.TopicListResponse, error) { return client.FetchTop(ctx, "weekly", page) })
			paginate("top_monthly", func(ctx context.Context, page int) (*forum.TopicListResponse, error) { return client.FetchTop(ctx, "monthly", page) })
			paginate("top_all", func(ctx context.Context, page int) (*forum.TopicListResponse, error) { return client.FetchTop(ctx, "all", page) })
			// Per-category 100% (copy cat for closure)
			for _, cat := range cats.CategoryList.Categories {
				catCopy := cat
				paginate(fmt.Sprintf("cat_%s", catCopy.Slug), func(ctx context.Context, page int) (*forum.TopicListResponse, error) {
					return client.FetchCategory(ctx, catCopy.ID, page)
				})
			}

			// Save topic list
			topics := make([]forum.Topic, 0, len(allTopics))
			for _, t := range allTopics {
				topics = append(topics, t)
			}
			if err := writeJSON(filepath.Join(outputDir, "topics.json"), topics); err != nil {
				return err
			}
			log.Info("topic list done, fetching posts", "topics", len(topics))

			// Fetch posts per topic concurrently (Go, 100% forum) — reuse AATB pattern
			postsDir := filepath.Join(outputDir, "topics")
			os.MkdirAll(postsDir, 0755)
			var totalPosts atomic.Int64
			sem := semaphore.NewWeighted(int64(concurrency))
			g, gctx := errgroup.WithContext(ctx)
			for _, t := range topics {
				t := t
				if err := sem.Acquire(gctx, 1); err != nil {
					break
				}
				g.Go(func() error {
					defer sem.Release(1)
					tr, err := client.FetchTopic(gctx, t.ID)
					if err != nil {
						log.Warn("fetch topic failed", "id", t.ID, "title", t.Title, "err", err)
						return nil
					}
					totalPosts.Add(int64(len(tr.PostStream.Posts)))
					path := filepath.Join(postsDir, fmt.Sprintf("%d-%s.json", t.ID, sanitize(t.Slug)))
					if err := writeJSON(path, tr); err != nil {
						log.Warn("write topic failed", "path", path, "err", err)
					}
					return nil
				})
			}
			_ = g.Wait()
			tp := int(totalPosts.Load())

			log.Info("forum scrape complete", "topics", len(topics), "posts", tp, "output", outputDir)
			fmt.Printf("\nDone. %d topics, %d posts -> %s\n", len(topics), tp, outputDir)
			// Generate simple ingest sql
			ingest := fmt.Sprintf("-- Forum ingest\n-- duckdb -c \"SELECT count(*) FROM read_json('%s/topics/*.json')\"\n", outputDir)
			os.WriteFile(filepath.Join(outputDir, "ingest.duckdb.sql"), []byte(ingest), 0644)
			return nil
		},
	}
	cmd.Flags().StringVar(&forumURL, "forum-url", "https://example.com", "Discourse base URL")
	cmd.Flags().StringVar(&outputDir, "output", "./forum-output", "Output dir")
	cmd.Flags().StringVar(&cookieFile, "cookie-file", "", "Path to HAR file or raw cookie file (~/forum.har)")
	cmd.Flags().StringVar(&cookieStr, "cookie", "", "Raw Cookie header value")
	cmd.Flags().IntVar(&concurrency, "concurrency", 8, "Workers (not yet used for posts, sequential for now)")
	cmd.Flags().Bool("verbose", false, "Verbose")
	return cmd
}

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func sanitize(s string) string {
	if s == "" {
		return "untitled"
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			out = append(out, c)
		} else {
			out = append(out, '_')
		}
		if len(out) > 80 {
			break
		}
	}
	return string(out)
}
