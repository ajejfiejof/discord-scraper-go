# Usage — DiscordScraperGo

## Install

```bash
git clone https://github.com/ajejfiejof/discord-scraper-go
cd discord-scraper-go
go build -o bin/scraper ./cmd/scraper
# or: make build  -> bin/scraper 11M, go1.27
```

Requires `go1.21+` (`cobra`, `golang.org/x/sync`, `fraugster/parquet-go` optional).

## Auth

User token (as Discrub does) or `Bot <token>`:

```bash
export DISCORD_TOKEN="your_token_here"  # or TOKEN
./bin/scraper inspect                    # lists guilds
./bin/scraper inspect --guild 123456789012345678  # lists channels
```

`--token` flag overrides env. Never commit tokens — `.gitignore` has `*.token`, `.env`, `output/`.

## Quick Start

```bash
# Whole guild (T440p tuned: 8 workers, 36 RPS, resume, duckdb, text-first)
./bin/scraper --guild 123456789012345678 --output ./output --concurrency 8 --target-rps 36 --resume --duckdb --no-media --verbose

# Single channel
./bin/scraper --channel 987654321098765432 --output ./output

# Date/content filter (snowflake ID compare, no time.Parse hot path)
./bin/scraper --guild 123... --after 2024-01-01 --before 2024-06-01 --content "hello" --format csv

# Dry run
./bin/scraper --guild 123... --dry-run --verbose
```

## Flags

| Flag | Default | Description |
|---|---|---|
| `--token` | `DISCORD_TOKEN` | Discord token (`Bot <token>` or raw user) |
| `--guild` | — | Guild ID (scrapes `106` text-like channels) |
| `--guilds` | — | Comma-separated guild IDs |
| `--channel` | — | Single channel ID |
| `--channels` | — | Comma-separated channel IDs |
| `--output` | `./output` | Output dir |
| `--format` | `jsonl` | `jsonl` (streaming, default), `json`, `csv`, `parquet` |
| `--concurrency` | `8` | Workers `1-64` (T440p: `8` = 1/thread) |
| `--target-rps` | `36` | Global `RPS` `20-45`, Discord `50` (ban-safe) |
| `--before`/`--after` | — | `YYYY-MM-DD` or `RFC3339` |
| `--content` | — | Substring `case-insensitive` (Sparser on raw bytes) |
| `--include-threads` | `true` | Archived + active threads (`~180`) |
| `--include-users` | `true` | Fetch `users.json` for all visible authors |
| `--include-reactions` | `false` | Fetch reactions per message (extra `429` load) |
| `--download-attachments` | `false` | Download media with `20GB` budget |
| `--media-budget-gb` | `20` | Media cap `GB` (`0` = disabled) |
| `--no-media` | `false` | Text-only (fastest, ban-safest) |
| `--resume` | `false` | Resume from `.checkpoint.json` |
| `--duckdb` | `false` | Generate `ingest.duckdb.sql` + `query.sh` |
| `--parquet` | `false` | Write `Parquet` `SNAPPY` `100M` row-groups |
| `--verbose` | `false` | `slog.LevelDebug` |
| `--dry-run` | `false` | Validate and exit |

## Resume

`.checkpoint.json` per `guild/channel` `{last_id, count, updated}` flushed every `20` batches. Re-run same command with `--resume` to continue from `last_id`. Safe to `Ctrl+C`.

## Media

Text-first: `--no-media` (default) skips `cdn.discordapp.com` entirely. With `--download-attachments`, `MediaBudget` `TryReserve(sizeHint)` caps at `20GB` (`--media-budget-gb`), `mediaSem 4` concurrent, `Exceeded` warns and skips further.

## Output

```
output/
  123456789012345678/
    guild.json, channels.json, users.json (if --include-users)
    111111111111111111-general.jsonl  # per-channel NDJSON
    media/<channel>/<msg>-<filename>  # if --download-attachments
  .checkpoint.json
  ingest.duckdb.sql, query.sh  # if --duckdb
```

Each `jsonl` line is a full `Message` JSON. Use `jq`, `duckdb read_json`, or `pandas.read_json(..., lines=True)`.

## Examples

```bash
# Inspect
./bin/scraper inspect --guild 123...

# Filtered + reactions
./bin/scraper --guild 123... --content "union" --include-reactions --format jsonl

# Parquet for DuckDB
./bin/scraper --guild 123... --format parquet --duckdb
duckdb -c "SELECT * FROM read_parquet('output/**/*.parquet') LIMIT 5"
```

See `docs/ARCHITECTURE.md`, `docs/DATABASE.md`, `docs/RATE_LIMITS.md`.
