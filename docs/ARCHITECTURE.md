# Architecture — DiscordScraperGo

> **Optimized for ThinkPad T440p (i7-4712MQ 4C/8T, AVX2, 8GB) — but scales to any modern box.**

This document explains the *why* behind the throughput numbers in `README.md`.

## Overview

```
cmd/scraper      Cobra CLI, flag parsing, auth check, signal handling, checkpoint wiring
internal/discord REST client (h2, keep-alive) + typed models (Guild/Channel/Message/User/...)
internal/ratelimit AATB limiter (global token bucket + per-bucket Remaining/Reset)
internal/extractor Orchestrator: discovery → filter → worker pool → streaming writer
internal/writer    Streaming NDJSON/CSV/Parquet + DuckDB ingest + MediaBudget 20GB
internal/checkpoint Resume (.checkpoint.json per guild/channel last_id)
pkg/snowflake    Discord snowflake ↔ time (epoch 1420070400000, shift 22)
```

## 1. Zero-Copy Stream Ingestion (`internal/writer/stream.go:212`)

**Problem:** Classic path `io.ReadAll → json.Unmarshal([]Message) → json.Marshal` per batch = `526` allocs, `369K` B/op, GC pauses on 8GB.

**Solution:** `ParseRawMessages` scans raw `[...] ` array *without* unmarshaling: state machine tracks `depth`/`inString`/`escaped`, slices `msgBytes` (`objStart:i+1`) directly from the wire buffer. `WriteRawBatch` does `cf.bw.Write(msgBytes)+'\n'` — no reflection, no `Message` structs. `Sparser` (`PVLDB 2018`) filter `StreamFilter.Match` runs on `idBytes`/`msgBytes` via `bytes.Compare` + `bytes.Contains` (AVX2 in Go runtime) before any `json.Unmarshal`.

**Result:** `BenchmarkZeroCopyStreamPipeline-8` `214k ns/op 22K B/op 10 allocs` vs `BenchmarkClassicReflectionPipeline-8` `842k ns/op 369K B/op 526 allocs` — **3.9× faster, 16× less RAM, 52× fewer allocs**. `55` bytes/batch is the transmute cost. `64 KiB` `bufio.Writer` fits Haswell `L2 256K/core` (was `256K` → thrashing).

## 2. AATB Rate Limiting (`internal/ratelimit/limiter.go:1`)

Based on *"Rethinking HTTP API Rate Limiting: A Client-Side Approach"* (arXiv:2510.04516, IEEE CCNC 2026) + *HiveMind*.

- **Continuous token bucket:** `tokens += elapsed * targetRPS`, `maxBurst 15`, `lastRefill` — eliminates artificial `nextAllowed` delays when burst available.
- **Major-param isolation:** `pathBucketKey` returns `/channels/:id/messages` for channel routes, `/guilds/:id/channels` for guild routes — prevents `channel A` 429 from blocking `channel B` (was generic `:id` scrub).
- **Telemetry-aware:** `UpdateFromHeaders` reads `X-RateLimit-Remaining/Limit/Reset-After` + `Register429` adds `10-30ms` jitter + `10%` safety. `globalUntil` for `global:true` 429s.
- **Defaults:** `32-36 RPS` ban-safe (Discord `50`), `8` workers = `1` per logical thread on `i7-4712MQ`.

## 3. HTTP Transport (`internal/discord/client.go:1`)

- `MaxIdleConns 128`, `MaxIdleConnsPerHost 32`, `IdleConnTimeout 90s`, `ForceAttemptHTTP2 true`, `ProxyFromEnvironment`, `DialContext 10s/KeepAlive 30s`.
- `BufferPool sync.Pool` of `32K` chunks for `doRaw()` — reuses across `8` workers, no `io.ReadAll` alloc.
- `do()` vs `doRaw()`: `do()` unmarshals to structs (for `Guild/Channel`), `doRaw()` returns `[]byte + release()` for messages (zero-copy path).

## 4. Extractor (`internal/extractor/extractor.go:1`)

- **Discovery:** `FetchGuild`, `FetchGuildChannels`, then `WriteGuildMeta` immediately → **DB hot in seconds**, not after `80s` of thread discovery. Main `106` text-like channels scraped first, then `discoverArchivedThreads` (bounded `sem 8`) adds `~180` threads.
- **Pagination:** per-channel sequential `before` cursor, `100`/`50` limit, `snowflake.FromTime` bounds for date filters (no `time.Parse` hot path), `compareSnowflake` string compare.
- **Worker pool:** `semaphore.Weighted(concurrency)` + `errgroup.WithContext` — `403 Missing Access` is non-fatal (`isPermissionError` checks `403/50001/50013`), only `context cancel` aborts.
- **Checkpoint:** `.checkpoint.json` `{guild/channel: {last_id, count, updated}}` every `20` batches, `resume` loads `before` cursor.
- **Media:** `MediaBudget 20GB` atomic `TryReserve`, `mediaSem 4` concurrent `Download` via `cdn.discordapp.com`, text-first.
- **Reactions/Users:** optional `FetchReactions` per emoji + `EnrichUsers` scans JSONL for unique `author`/`mentions` → `FetchUser` `8` concurrent → `users.json`.

## 5. Writer (`internal/writer/*.go`)

- `Writer` per-channel `bufio 64K` + `json.Encoder` or `csv.Writer`, `sync.Mutex` per file, `count` for stats.
- `ParquetWriter` (`floor` + `SNAPPY 100M` row-groups, `ParquetRow` schema) for `read_parquet`, but default is `JSONL` → `GenerateDuckDBArtifacts()` writes `ingest.duckdb.sql` (`PRAGMA threads 8`, `CREATE TABLE messages AS SELECT ... FROM read_json('**/*.jsonl')`, indexes).
- `MediaBudget` tracks `used/limit`, `Download` respects `sizeHint` reservation.

## 6. Hardware Notes (T440p)

- `4C/8T` → `8` workers saturates without thrashing; `8GB` → `64K` buf × `8` = `512K` total, fits `L2`; `AVX2` accelerates `bytes.Contains` in `Sparser`.
- Scales: `16-24` workers on `16T` desktop, `36 RPS` still ban-safe.

## Further Reading

- `docs/RATE_LIMITS.md` — ban-safe tuning
- `docs/DATABASE.md` — DuckDB queries
- `docs/USAGE.md` — flags & examples
