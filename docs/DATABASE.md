# Database — DuckDB & Parquet

Scraper is text-first (`--no-media`), `output/**/*.jsonl` is the DB. DuckDB reads it natively — no `COPY` needed until you want a persistent file.

## Quick Query (no ingest)

```bash
# Requires duckdb CLI https://duckdb.org/docs/installation/
duckdb -c "SELECT count(*) FROM read_json('output/**/*.jsonl', auto_detect=true)"
duckdb -c "SELECT channel_id, count(*) c FROM read_json('output/**/*.jsonl') GROUP BY 1 ORDER BY c DESC LIMIT 15"
duckdb -c "SELECT * FROM read_json('output/**/*.jsonl') WHERE content ILIKE '%union%' LIMIT 10"
```

## Persistent DB

`--duckdb` generates `output/ingest.duckdb.sql` + `output/query.sh`:

```sql
-- output/ingest.duckdb.sql (auto-generated, PRAGMA threads 8)
PRAGMA threads=8;
PRAGMA memory_limit='2GB';
DROP TABLE IF EXISTS messages;
CREATE TABLE messages AS
SELECT id::VARCHAR AS id, channel_id::VARCHAR, author.id::VARCHAR AS author_id,
       author.username::VARCHAR, content::VARCHAR, timestamp::TIMESTAMPTZ AS timestamp
FROM read_json('output/**/*.jsonl', auto_detect=true, ignore_errors=true, maximum_object_size=16777216);
CREATE INDEX idx_messages_channel ON messages(channel_id);
```

```bash
./output/query.sh          # creates output/output.duckdb
duckdb output/output.duckdb -c "SELECT date_trunc('day', timestamp) AS day, count(*) FROM messages GROUP BY day ORDER BY day"
```

## Parquet

`--parquet` or `--format parquet` writes `output/**/*.parquet` (`floor` + `SNAPPY 100M` row-groups, `ParquetRow` schema: `id, channel_id, guild_id, author_id, content, timestamp`). Query via:

```bash
duckdb -c "SELECT * FROM read_parquet('output/**/*.parquet') LIMIT 5"
```

Parquet is `2-3×` smaller than `jsonl` after `zstd`, better for archival.

## Compression

```bash
tar -I 'zstd -19' -cf archive.tar.zst output/
# 1.6G jsonl → ~400M zstd
```

## Users & Media

- `output/<guild>/users.json` (`--include-users`, default `true`): `{guild, count, users{ id: User }}` from `FetchUser` per unique author.
- `output/<guild>/media/<channel>/<msg>-<filename>` (`--download-attachments`, `20GB` cap, `MediaBudget` atomic).

## Checkpoint

`output/.checkpoint.json` `{guild/channel: {last_id, count, updated}}` — resume-safe, `sort -u` by `id` to dedup if needed.
