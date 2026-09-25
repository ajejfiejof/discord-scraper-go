# DiscordScraperGo

Blazing-fast, highly concurrent Discord scraper in Go — inspired by [Discrub](https://github.com/Ashley Melanie Brooks. See LICENSE.

> **Goal:** extract **all** messages + metadata from a guild (server) at the highest sustainable throughput Discord's API allows, without the browser-extension overhead of Discrub.

---

## Why Go?

| Discrub (TypeScript / Extension) | DiscordScraperGo (Go) |
|---|---|
| Sequential `fetch` + artificial `searchDelay` | **Bounded worker pools** (semaphore + errgroup), 8-64 concurrent channels |
| In-memory message arrays | **Streaming NDJSON** per channel (256 KiB buf, no RAM blowup) |
| Global `setTimeout` for 429 | **Token-bucket limiter** parsing `X-RateLimit-*` headers per bucket + `retry_after` |
| Single `http` client | Tuned `http.Transport` (h2, 256 idle conns, 90s idle timeout, keep-alive reuse) |
| Search endpoint (offset pagination, 25/page) | **Primary path:** `GET /channels/:id/messages?before=&limit=100` (4× faster, no search quota) + fallback search filter |

---

## Architecture

```
cmd/scraper      CLI (Cobra) — flag parsing, auth check, signal handling
internal/discord REST client (rate-limited, retry, http2) + typed models (User/Guild/Channel/Message/...)
internal/ratelimit Per-bucket + global limiter (mutex + wait queues)
internal/extractor Orchestrator:
                    1. Discovery: /guilds/:id/channels + /guilds/:id/threads/active + per-channel archived threads
                    2. Filter to text-like channels (text, announcement, forum, media, voice, threads)
                    3. Worker pool over channels (semaphore N) — each channel paginates sequentially via `before` cursor
                    4. Stream batches to writer
internal/writer    Streaming writer — per-channel `guildID/channelID-name.jsonl` (or CSV), guild.json, channels.json
pkg/snowflake      Snowflake ↔ time conversion (mirrors Discrub's generateSnowflake)
```

### Rate-limit strategy

- **Bucket-aware:** `X-RateLimit-Bucket` → `map[bucket]*bucket{remaining, resetAt}`. `Wait()` before every request blocks on `globalUntil` or bucket reset.
- **Header-driven:** after each response, `UpdateFromHeaders` records `Remaining/Limit/Reset-After`.
- **429 handling:** parse `retry_after` JSON + `Retry-After` header, register `Register429` on both bucket and fallback route key (method + scrubbed path). Global 429 sets `globalUntil`.
- **5xx backoff:** exponential `300ms * 2^attempt` up to 5s, up to 12 total retries.

### Throughput knobs

- Default `--concurrency 16` (1-64). Discord global ≈ 50 req/s, per-channel ≈ 5 req/s — 16-24 is the sweet spot.
- Each channel does `limit=100` per request. 1M messages in a channel ≈ 10k requests, single worker does ~5 req/s → ~30 min solo; 16 workers across 50 channels overlap IO and hide latency.
- Buffered `bufio.Writer` (256 KiB) + `json.Encoder` reuse, `sync.Mutex` per file, `sync.Pool`-friendly.

---

## Quick start

```bash
# Clone & build
git clone https://github.com/Ashley Melanie Brooks. See LICENSE.
cd discord-scraper-go
go build -o bin/scraper ./cmd/scraper

# Set token (user token as Discrub does, or Bot <token>)
export DISCORD_TOKEN="your_token_here"

# Inspect what you can access
./bin/scraper inspect                       # lists guilds
./bin/scraper inspect --guild 123...        # lists channels for guild

# Scrape a guild (all channels + threads) -> ./output/<guildID>/
./bin/scraper --guild 123456789012345678 --output ./output --concurrency 24

# Scrape single channel
./bin/scraper --channel 987654321098765432 --output ./output

# With filters and CSV
./bin/scraper --guild 123... --after 2024-01-01 --before 2024-06-01 --content "hello" --format csv

# Dry run (validate without fetching)
./bin/scraper --guild 123... --dry-run --verbose
```

### Flags

| Flag | Description |
|---|---|
| `--token` | Discord token (or `DISCORD_TOKEN` / `TOKEN` env). Use `Bot <token>` for bots, raw for user tokens |
| `--guild` | Guild (server) ID — scrapes all text-like channels |
| `--guilds` | Comma-separated guild IDs |
| `--channel` | Single channel ID |
| `--channels` | Comma-separated channel IDs |
| `--output` | Output dir (default `./output`) |
| `--format` | `jsonl` (default, one JSON per line, streaming), `json`, `csv` |
| `--concurrency` | Concurrent channel workers 1-64 (default 16) |
| `--before` / `--after` | Date filter (`YYYY-MM-DD` or RFC3339) — client-side fast path |
| `--content` | Substring filter (case-insensitive) |
| `--include-threads` | Include archived + active threads (default true) |
| `--verbose` | Debug logs |
| `--dry-run` | Validate and exit |

---

## Output layout

```
output/
  123456789012345678/                 # guild ID
    guild.json                        # full guild object
    channels.json                     # all channels (including non-text)
    111111111111111111-general.jsonl   # per-channel NDJSON (channelID-name)
    222222222222222222-random.jsonl
    333333333333333333-thread-xyz.jsonl
  dm/
    444444444444444444.jsonl            # DM channels when scraping --channel
```

Each `.jsonl` line is a complete `Message` JSON (same shape as Discord `Message` object + `User`, `Attachment`, `Embed`, etc.). Use `jq`, `duckdb`, or `pandas.read_json(..., lines=True)`.

- `guild.json` — `Guild` object (roles, emojis, features, etc.)
- `channels.json` — `[]Channel` as returned by `/guilds/:id/channels`

CSV mode flattens key fields: `id,channel_id,author_id,author_username,content,timestamp,edited_timestamp,pinned,type,attachments,embeds`.

---

## Discrub parity checklist

- [x] `generateSnowflake` / date → snowflake (`pkg/snowflake`)
- [x] Guilds, channels, roles, DMs, threads (archived private/public + joined + active)
- [x] Message pagination (`limit=100&before=ID`, 50 for `around`)
- [x] Search endpoint (`/guilds/:id/messages/search` with `min_id/max_id/content/has/author_id/mentions/channel_id/pinned/offset`)
- [x] Reactions (`/channels/:id/messages/:id/reactions/:emoji`)
- [x] Delay/429 handling (`retry_after` loop)
- [ ] Attachment download (stream `downloadFile` via CDN URL) — stubbed, ready to add `writer.DownloadAttachments`
- [ ] Reaction + user lookups expansion — hook exists (`excludeReactions` / `excludeUserLookups` pattern)
- [ ] Predefined export presets (like Discrub's format dropdown) — use `--format` for now

---

## Performance tips

- **Concurrency:** start at 16, raise to 24-32 if your guild has >100 channels. Monitor `X-RateLimit-Remaining` in `--verbose` logs — if every request 429s, lower concurrency.
- **Disk:** use NVMe/SSD for output; NDJSON streaming still needs `fsync` per `Flush()`.
- **Token type:** user tokens get higher rate limits than bot tokens on message history endpoints (Discrub behavior). Bot tokens work but may be slightly throttled.
- **Resume:** re-running with same output dir **appends** (no dedup yet). For resumable exports, track last `before` per channel yourself or dedup by `id` (`sort -u`).

---

## Development

```bash
make vet          # go vet ./...
make build        # -> bin/scraper
make test         # go test -race -cover
make lint         # golangci-lint (if installed)
make tidy         # go mod tidy
```

Go 1.21+ required (tested 1.27). Dependencies: `cobra`, `golang.org/x/sync`.

### Project layout

```
cmd/scraper/main.go         CLI entry
internal/config/config.go   Config + validation
internal/discord/           Models + client + API methods
internal/ratelimit/         Bucket/global limiter
internal/extractor/         Worker pool + discovery
internal/writer/            Streaming output
pkg/snowflake/              Snowflake helpers
Makefile, go.mod, .gitignore, README.md
```

---

## Token safety

- Never commit tokens. Use env `DISCORD_TOKEN`.
- `.gitignore` excludes `*.token`, `.env`, `output/`, `config.*`.
- User-token scraping may violate Discord ToS for automation — use at your own risk; this tool is for **personal data export** (like Discrub) analogous to GDPR export.

---

## License

AGPLv3 — Copyright (C) 2026 Ashley Melanie Brooks. See [LICENSE](LICENSE).

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. Network use requires source disclosure.

Previous MIT history relicensed with sole contributor consent.
